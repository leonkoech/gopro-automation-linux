#!/usr/bin/env python3
"""
Thin CLI wrapper around VideoUploadService for bash script usage.
Handles all SSL/ARM workarounds that the aws CLI cannot.

Usage:
    python3 s3_upload_helper.py <file_path> <s3_key>
    python3 s3_upload_helper.py --verify   (test credentials)

Reads config from .env (same file as main app).
Prints progress lines to stdout: PROGRESS:<percent>
Prints result to stdout: OK:<s3_uri>  or  FAIL:<error>
"""
import sys
import os

# SSL fixes MUST come before any other imports (same as videoupload.py)
os.environ['OPENSSL_CONF'] = '/dev/null'
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import ssl
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import urllib3.util.ssl_
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers('DEFAULT@SECLEVEL=1')
    ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
    urllib3.util.ssl_.DEFAULT_CIPHERS = 'DEFAULT@SECLEVEL=1'
except Exception:
    pass

from dotenv import load_dotenv

# Load .env from same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import botocore.httpsession

# Patch botocore SSL (same as videoupload.py)
try:
    original_get_cert_path = botocore.httpsession.get_cert_path
    def patched_ssl_context(*args, **kwargs):
        return False
    botocore.httpsession.get_cert_path = patched_ssl_context
except Exception:
    pass


def create_s3_client():
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('UPLOAD_REGION', 'us-east-1')

    if not aws_key or not aws_secret:
        print("FAIL:AWS credentials not found in .env", flush=True)
        sys.exit(1)

    boto_config = BotoConfig(
        retries={'max_attempts': 10, 'mode': 'adaptive'},
        connect_timeout=60,
        read_timeout=300,
        max_pool_connections=1,
        tcp_keepalive=True
    )

    client = boto3.client(
        's3',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
        config=boto_config,
        verify=False
    )

    transfer_config = TransferConfig(
        multipart_threshold=50 * 1024 * 1024,
        max_concurrency=1,
        multipart_chunksize=25 * 1024 * 1024,
        use_threads=False,
        max_io_queue=1,
        num_download_attempts=10
    )

    return client, transfer_config


def verify_credentials():
    try:
        client, _ = create_s3_client()
        bucket = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
        client.head_bucket(Bucket=bucket)
        print("OK:credentials_valid", flush=True)
    except Exception as e:
        print(f"FAIL:{e}", flush=True)
        sys.exit(1)


def upload_file(file_path, s3_key):
    bucket = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
    client, transfer_config = create_s3_client()

    if not os.path.exists(file_path):
        print(f"FAIL:File not found: {file_path}", flush=True)
        sys.exit(1)

    file_size = os.path.getsize(file_path)

    class Progress:
        def __init__(self):
            self.uploaded = 0
            self.last_pct = -1

        def __call__(self, bytes_amount):
            self.uploaded += bytes_amount
            pct = int((self.uploaded / file_size) * 100) if file_size > 0 else 100
            if pct != self.last_pct:
                self.last_pct = pct
                print(f"PROGRESS:{pct}", flush=True)

    progress = Progress()

    max_retries = 3
    last_error = None

    for attempt in range(max_retries):
        try:
            progress.uploaded = 0
            progress.last_pct = -1

            client.upload_file(
                file_path,
                bucket,
                s3_key,
                Callback=progress,
                ExtraArgs={'ContentType': 'video/mp4'},
                Config=transfer_config
            )

            s3_uri = f"s3://{bucket}/{s3_key}"
            print(f"OK:{s3_uri}", flush=True)
            return

        except Exception as e:
            last_error = e
            error_str = str(e).lower()
            if 'ssl' in error_str or 'eof' in error_str or 'protocol' in error_str:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue
            print(f"FAIL:{e}", flush=True)
            sys.exit(1)

    print(f"FAIL:All {max_retries} retries failed: {last_error}", flush=True)
    sys.exit(1)


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
        print("FAIL:Missing arguments. Need: <file_path> <s3_key>")
        sys.exit(1)
