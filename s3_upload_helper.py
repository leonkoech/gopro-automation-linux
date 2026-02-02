#!/usr/bin/env python3
"""
S3 upload helper using presigned URLs + curl for data transfer.
Bypasses Python's broken SSL on ARM/Jetson by using curl for heavy lifting.

Usage:
    python3 s3_upload_helper.py <file_path> <s3_key>
    python3 s3_upload_helper.py --verify

Reads config from .env. Prints progress to stdout.
"""
import sys
import os
import subprocess
import math
import json
import time

# SSL fixes for the small boto3 API calls (create/complete multipart)
os.environ['OPENSSL_CONF'] = '/dev/null'
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import ssl
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import urllib3.util.ssl_ as urllib3_ssl
    urllib3_ssl.DEFAULT_CIPHERS = 'DEFAULT@SECLEVEL=1'
    _orig = urllib3_ssl.create_urllib3_context
    def _patched(ssl_version=None, cert_reqs=None, *a, **kw):
        ctx = _orig(ssl_version, cert_reqs, *a, **kw)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        try: ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        except: pass
        try: ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        except: pass
        return ctx
    urllib3_ssl.create_urllib3_context = _patched
except Exception:
    pass

from dotenv import load_dotenv
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import botocore.httpsession
try:
    botocore.httpsession.get_cert_path = lambda *a, **k: False
except Exception:
    pass

# Part size for multipart upload: 25 MB
PART_SIZE = 25 * 1024 * 1024
# Simple PUT limit: 4.5 GB (S3 max single PUT is 5 GB)
SIMPLE_PUT_LIMIT = 4500 * 1024 * 1024
MAX_RETRIES = 5


def create_s3_client():
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('UPLOAD_REGION', 'us-east-1')
    if not aws_key or not aws_secret:
        print("FAIL:AWS credentials not found in .env", flush=True)
        sys.exit(1)

    client = boto3.client(
        's3',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
        config=BotoConfig(
            retries={'max_attempts': 10, 'mode': 'adaptive'},
            connect_timeout=60,
            read_timeout=300,
            max_pool_connections=1,
            signature_version='s3v4',
        ),
        verify=False
    )
    return client


def verify_credentials():
    try:
        client = create_s3_client()
        bucket = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
        client.head_bucket(Bucket=bucket)
        print("OK:credentials_valid", flush=True)
    except Exception as e:
        print(f"FAIL:{e}", flush=True)
        sys.exit(1)


def _get_temp_dir(file_path):
    """Use same directory as the file for temp storage (avoids tmpfs/RAM issues on Jetson)."""
    d = os.path.dirname(os.path.abspath(file_path))
    tmp = os.path.join(d, '.upload_tmp')
    os.makedirs(tmp, exist_ok=True)
    return tmp


def _cleanup_temp_dir(file_path):
    """Remove the temp directory after upload."""
    tmp = os.path.join(os.path.dirname(os.path.abspath(file_path)), '.upload_tmp')
    try:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    except Exception:
        pass


def _curl_simple_upload(file_path, presigned_url, file_size):
    """Upload entire file with a single PUT using presigned URL. Returns True or raises."""
    tmp_dir = _get_temp_dir(file_path)
    header_file = os.path.join(tmp_dir, 'put_headers.txt')
    body_file = os.path.join(tmp_dir, 'put_body.txt')

    for attempt in range(MAX_RETRIES):
        try:
            curl_cmd = [
                'curl', '-s', '-S',
                '--insecure',
                '--tlsv1.2', '--tls-max', '1.2',
                '--upload-file', file_path,       # Streams from disk, implies PUT
                '-H', 'Content-Type: video/mp4',
                '-H', 'Expect:',                  # Suppress 100-continue (critical!)
                '--retry', '2',
                '--retry-delay', '5',
                '--retry-all-errors',
                '--connect-timeout', '30',
                '--speed-limit', '1024',           # Abort if < 1KB/s
                '--speed-time', '60',              # ... for 60 seconds
                '--max-time', '7200',              # 2 hour max for large files
                '-D', header_file,
                '-o', body_file,
                '-w', '%{http_code}',
                presigned_url
            ]

            result = subprocess.run(curl_cmd, capture_output=True, timeout=7260)
            http_code = result.stdout.decode().strip()

            if http_code == '200':
                return True
            else:
                body = ''
                try:
                    with open(body_file, 'r') as f:
                        body = f.read()[:500]
                except Exception:
                    pass
                stderr_text = result.stderr.decode().strip()
                raise Exception(f"HTTP {http_code} (curl exit {result.returncode}): {stderr_text} | {body}")

        except subprocess.TimeoutExpired:
            print(f"PUT_ERR:Simple PUT attempt {attempt+1}/{MAX_RETRIES}: timeout", flush=True, file=sys.stderr)
        except Exception as e:
            print(f"PUT_ERR:Simple PUT attempt {attempt+1}/{MAX_RETRIES}: {e}", flush=True, file=sys.stderr)
            if attempt >= MAX_RETRIES - 1:
                raise

        finally:
            for f in [header_file, body_file]:
                try: os.unlink(f)
                except Exception: pass

        time.sleep(min(2 ** attempt, 30))

    raise Exception("Simple PUT failed after all retries")


def _curl_upload_part(file_path, presigned_url, offset, length, part_num, total_parts):
    """Upload a single part via curl, return ETag or None."""
    tmp_dir = _get_temp_dir(file_path)

    for attempt in range(MAX_RETRIES):
        part_file = os.path.join(tmp_dir, f'part{part_num}.dat')
        header_file = os.path.join(tmp_dir, f'part{part_num}.headers')
        body_file = os.path.join(tmp_dir, f'part{part_num}.body')

        try:
            # Write part data to file on same disk (not /tmp which may be RAM-backed)
            with open(file_path, 'rb') as src, open(part_file, 'wb') as dst:
                src.seek(offset)
                remaining = length
                while remaining > 0:
                    chunk = src.read(min(remaining, 8 * 1024 * 1024))  # 8MB read chunks
                    if not chunk:
                        break
                    dst.write(chunk)
                    remaining -= len(chunk)

            # Verify part file was written correctly
            actual_size = os.path.getsize(part_file)
            if actual_size != length:
                raise Exception(f"Part file size mismatch: expected {length}, got {actual_size}")

            # Key fixes vs previous version:
            #   - No -X PUT (--upload-file already implies PUT)
            #   - No Content-Type header (not needed for upload_part, avoids signature mismatch)
            #   - Added -H "Expect:" to suppress 100-continue (most likely culprit!)
            #   - Added --speed-limit/--speed-time for stall detection
            #   - Capture response body on failure for debugging
            curl_cmd = [
                'curl', '-s', '-S',
                '--insecure',
                '--tlsv1.2', '--tls-max', '1.2',
                '--upload-file', part_file,
                '-H', 'Expect:',                  # Suppress Expect: 100-continue
                '--retry', '2',
                '--retry-delay', '3',
                '--retry-all-errors',
                '--connect-timeout', '30',
                '--speed-limit', '1024',           # Abort if < 1KB/s
                '--speed-time', '60',              # ... for 60 seconds
                '--max-time', '600',               # 10 min timeout per part
                '-D', header_file,
                '-o', body_file,
                '-w', '%{http_code}',
                presigned_url
            ]

            result = subprocess.run(curl_cmd, capture_output=True, timeout=660)
            http_code = result.stdout.decode().strip()
            stderr_text = result.stderr.decode().strip()

            if http_code == '200':
                # Parse ETag from response headers
                etag = None
                try:
                    with open(header_file, 'r') as hf:
                        for line in hf:
                            if line.lower().startswith('etag:'):
                                etag = line.split(':', 1)[1].strip().strip('"')
                                break
                except Exception:
                    pass

                if etag:
                    return etag
                else:
                    raise Exception("HTTP 200 but no ETag in response headers")

            else:
                body = ''
                try:
                    with open(body_file, 'r') as f:
                        body = f.read()[:500]
                except Exception:
                    pass
                raise Exception(f"HTTP {http_code} (curl exit {result.returncode}): {stderr_text} | {body}")

        except subprocess.TimeoutExpired:
            print(f"PART_ERR:Part {part_num} attempt {attempt+1}/{MAX_RETRIES}: timeout", flush=True, file=sys.stderr)
        except Exception as e:
            print(f"PART_ERR:Part {part_num} attempt {attempt+1}/{MAX_RETRIES}: {e}", flush=True, file=sys.stderr)
            if attempt >= MAX_RETRIES - 1:
                return None

        finally:
            for f in [part_file, header_file, body_file]:
                try: os.unlink(f)
                except Exception: pass

        time.sleep(min(2 ** attempt, 30))

    return None


def upload_file(file_path, s3_key):
    """Upload a file to S3 using presigned URLs + curl."""
    bucket = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
    client = create_s3_client()

    if not os.path.exists(file_path):
        print(f"FAIL:File not found: {file_path}", flush=True)
        sys.exit(1)

    file_size = os.path.getsize(file_path)

    print(f"PROGRESS:0", flush=True)

    # For files under 4.5GB, try simple PUT first (proven to work on Jetson)
    if file_size < SIMPLE_PUT_LIMIT:
        try:
            presigned_url = client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': bucket,
                    'Key': s3_key,
                    'ContentType': 'video/mp4'
                },
                ExpiresIn=7200  # 2 hours
            )

            print(f"PROGRESS:5", flush=True)
            _curl_simple_upload(file_path, presigned_url, file_size)
            print(f"PROGRESS:100", flush=True)

            s3_uri = f"s3://{bucket}/{s3_key}"
            print(f"OK:{s3_uri}", flush=True)
            _cleanup_temp_dir(file_path)
            return

        except Exception as e:
            print(f"SIMPLE_FALLBACK:Simple PUT failed ({e}), trying multipart", flush=True, file=sys.stderr)

    # Multipart upload for large files (>4.5GB) or as fallback
    num_parts = math.ceil(file_size / PART_SIZE)

    # Step 1: Create multipart upload (small API call via boto3)
    try:
        mpu = client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            ContentType='video/mp4'
        )
        upload_id = mpu['UploadId']
    except Exception as e:
        print(f"FAIL:Could not create multipart upload: {e}", flush=True)
        _cleanup_temp_dir(file_path)
        sys.exit(1)

    # Step 2: Upload each part using curl with presigned URLs
    parts = []
    try:
        for part_num in range(1, num_parts + 1):
            offset = (part_num - 1) * PART_SIZE
            length = min(PART_SIZE, file_size - offset)

            # Generate presigned URL for this part (small API call via boto3)
            presigned_url = client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': bucket,
                    'Key': s3_key,
                    'UploadId': upload_id,
                    'PartNumber': part_num
                },
                ExpiresIn=3600
            )

            etag = _curl_upload_part(file_path, presigned_url, offset, length, part_num, num_parts)
            if etag is None:
                raise Exception(f"Part {part_num}/{num_parts} failed after retries")

            parts.append({'ETag': etag, 'PartNumber': part_num})

            pct = int(part_num * 100 / num_parts)
            print(f"PROGRESS:{pct}", flush=True)

    except Exception as e:
        # Abort the multipart upload on failure
        try:
            client.abort_multipart_upload(
                Bucket=bucket, Key=s3_key, UploadId=upload_id
            )
        except Exception:
            pass
        print(f"FAIL:{e}", flush=True)
        _cleanup_temp_dir(file_path)
        sys.exit(1)

    # Step 3: Complete multipart upload (small API call via boto3)
    try:
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    except Exception as e:
        print(f"FAIL:Complete multipart failed: {e}", flush=True)
        _cleanup_temp_dir(file_path)
        sys.exit(1)

    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"OK:{s3_uri}", flush=True)
    _cleanup_temp_dir(file_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 s3_upload_helper.py <file_path> <s3_key>")
        print("       python3 s3_upload_helper.py --verify")
        sys.exit(1)

    if sys.argv[1] == '--verify':
        verify_credentials()
    elif len(sys.argv) >= 3:
        upload_file(sys.argv[1], sys.argv[2])
    else:
        print("FAIL:Missing arguments")
        sys.exit(1)
