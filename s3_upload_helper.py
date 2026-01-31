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

# Part size for multipart upload: 100 MB
# Smaller parts = more requests but each is more likely to succeed
PART_SIZE = 100 * 1024 * 1024
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


def upload_part_with_curl(presigned_url, file_path, offset, length, part_num, total_parts):
    """Upload a single part using curl (bypasses Python SSL issues)."""
    for attempt in range(MAX_RETRIES):
        try:
            # Use dd to extract the part, pipe to curl
            dd_cmd = [
                'dd', f'if={file_path}',
                f'bs={1024*1024}',  # 1MB block size
                f'skip={offset // (1024*1024)}',
                f'count={math.ceil(length / (1024*1024))}',
                'status=none'
            ]

            curl_cmd = [
                'curl', '-s', '-S',
                '--insecure',          # Skip SSL verification
                '--tlsv1.2',           # Force TLS 1.2
                '--tls-max', '1.2',    # Cap at TLS 1.2
                '-X', 'PUT',
                '-H', 'Content-Type: application/octet-stream',
                '--data-binary', '@-',  # Read from stdin
                '--retry', '3',
                '--retry-delay', '2',
                '--connect-timeout', '30',
                '--max-time', '600',    # 10 min timeout per part
                '-w', '%{http_code}',
                '-o', '/dev/null',
                presigned_url
            ]

            # If the part is the last one, it may be smaller than a full MB block
            # Use exact byte count with head -c
            if length % (1024*1024) != 0:
                # dd gives us ceiling(length/1MB) * 1MB, pipe through head -c to get exact
                dd_proc = subprocess.Popen(dd_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                head_cmd = ['head', '-c', str(length)]
                head_proc = subprocess.Popen(head_cmd, stdin=dd_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                dd_proc.stdout.close()
                curl_proc = subprocess.Popen(curl_cmd, stdin=head_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                head_proc.stdout.close()
            else:
                dd_proc = subprocess.Popen(dd_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                head_proc = None
                curl_proc = subprocess.Popen(curl_cmd, stdin=dd_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                dd_proc.stdout.close()

            stdout, stderr = curl_proc.communicate(timeout=660)
            http_code = stdout.decode().strip()

            # Clean up
            dd_proc.wait()
            if head_proc:
                head_proc.wait()

            if http_code == '200':
                # Extract ETag from response headers — need to redo with headers
                # Actually, presigned upload_part returns ETag in headers
                # Let's redo the curl call to capture headers
                pass
            elif curl_proc.returncode == 0 and http_code.startswith('2'):
                pass
            else:
                raise Exception(f"HTTP {http_code}, curl exit {curl_proc.returncode}: {stderr.decode().strip()}")

        except subprocess.TimeoutExpired:
            if attempt < MAX_RETRIES - 1:
                import time
                time.sleep(2 ** attempt)
                continue
            print(f"FAIL:Part {part_num} timed out after {MAX_RETRIES} retries", flush=True)
            return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                import time
                time.sleep(2 ** attempt)
                continue
            print(f"FAIL:Part {part_num} failed: {e}", flush=True)
            return None

    # Need to get ETag — re-upload with header capture
    # Actually, let's use a simpler approach: capture headers with -D
    return True  # We'll use a different approach below


def upload_file(file_path, s3_key):
    """Upload a file to S3 using presigned multipart upload + curl."""
    bucket = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
    client = create_s3_client()

    if not os.path.exists(file_path):
        print(f"FAIL:File not found: {file_path}", flush=True)
        sys.exit(1)

    file_size = os.path.getsize(file_path)
    num_parts = math.ceil(file_size / PART_SIZE)

    print(f"PROGRESS:0", flush=True)

    # Step 1: Create multipart upload (small API call)
    try:
        mpu = client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            ContentType='video/mp4'
        )
        upload_id = mpu['UploadId']
    except Exception as e:
        print(f"FAIL:Could not create multipart upload: {e}", flush=True)
        sys.exit(1)

    # Step 2: Upload each part using curl with presigned URLs
    parts = []
    try:
        for part_num in range(1, num_parts + 1):
            offset = (part_num - 1) * PART_SIZE
            length = min(PART_SIZE, file_size - offset)

            # Generate presigned URL for this part
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

            # Upload part with curl, capturing ETag from response headers
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
        sys.exit(1)

    # Step 3: Complete multipart upload (small API call)
    try:
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    except Exception as e:
        print(f"FAIL:Complete multipart failed: {e}", flush=True)
        sys.exit(1)

    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"OK:{s3_uri}", flush=True)


def _curl_upload_part(file_path, presigned_url, offset, length, part_num, total_parts):
    """Upload a single part via curl, return ETag on success or None on failure."""
    import tempfile
    import time

    for attempt in range(MAX_RETRIES):
        header_file = tempfile.mktemp(suffix='.headers')
        try:
            # Build curl command
            curl_cmd = [
                'curl', '-s', '-S',
                '--insecure',
                '--tlsv1.2', '--tls-max', '1.2',
                '-X', 'PUT',
                '-H', 'Content-Type: application/octet-stream',
                '--data-binary', '@-',
                '--retry', '2',
                '--retry-delay', '3',
                '--connect-timeout', '30',
                '--max-time', '600',
                '-D', header_file,
                '-o', '/dev/null',
                '-w', '%{http_code}',
                presigned_url
            ]

            # Read the exact bytes for this part and pipe to curl
            with open(file_path, 'rb') as f:
                f.seek(offset)
                part_data = f.read(length)

            result = subprocess.run(
                curl_cmd,
                input=part_data,
                capture_output=True,
                timeout=660
            )

            http_code = result.stdout.decode().strip()

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
                    # If we can't parse ETag, the upload might still be OK
                    # but we can't complete the multipart without it
                    raise Exception(f"Got 200 but no ETag in response headers")

            else:
                stderr_text = result.stderr.decode().strip()
                raise Exception(f"HTTP {http_code}: {stderr_text}")

        except subprocess.TimeoutExpired:
            pass  # Will retry
        except Exception as e:
            if attempt >= MAX_RETRIES - 1:
                return None

        finally:
            try:
                os.unlink(header_file)
            except Exception:
                pass

        time.sleep(min(2 ** attempt, 30))

    return None


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
