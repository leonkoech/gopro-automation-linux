#!/usr/bin/env python3
"""
Upload script with MTU optimization for Jetson devices.
This version uses smaller TCP segments and keepalive settings
to work around SSL EOF issues that occur with large transfers.

Run with: python upload_mtu.py
Optionally pass --delete to delete segments after successful upload.

If this still fails, try reducing the system MTU:
    sudo ip link set eth0 mtu 1400
    # or for WiFi
    sudo ip link set wlan0 mtu 1400
"""

import os
import sys
import socket
import ssl
import hashlib
import hmac
import datetime
from pathlib import Path
from urllib.parse import quote, urlparse
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Configuration from environment
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
UPLOAD_BUCKET = os.getenv("UPLOAD_BUCKET", "jetson-videos-uai")
UPLOAD_LOCATION = os.getenv("UPLOAD_LOCATION", "default-location")
UPLOAD_DEVICE_NAME = os.getenv("UPLOAD_DEVICE_NAME", os.uname().nodename)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Segments directory
SEGMENTS_DIR = os.path.expanduser("~/gopro_videos/segments")

# Upload settings - smaller chunks help avoid SSL issues
SEND_CHUNK_SIZE = 64 * 1024  # 64KB - small chunks for more reliable SSL
READ_BUFFER_SIZE = 1024 * 1024  # 1MB read buffer


def get_sessions():
    """Get all segment sessions with files."""
    segments_path = Path(SEGMENTS_DIR)
    if not segments_path.exists():
        print(f"Segments directory not found: {SEGMENTS_DIR}")
        return []

    sessions = []
    for session_dir in sorted(segments_path.iterdir()):
        if session_dir.is_dir():
            video_files = list(session_dir.glob("*.mp4")) + list(session_dir.glob("*.MP4"))
            if video_files:
                total_size = sum(f.stat().st_size for f in video_files)
                sessions.append({
                    'name': session_dir.name,
                    'path': str(session_dir),
                    'files': video_files,
                    'size_gb': total_size / (1024**3)
                })
    return sessions


def parse_session_date(session_name):
    """Extract date from session name (format: interfaceId_YYYYMMDD_HHMMSS)."""
    parts = session_name.split('_')
    if len(parts) >= 2:
        date_str = parts[-2]  # YYYYMMDD
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return None


def extract_camera_name(session_name):
    """Extract camera identifier from session name."""
    parts = session_name.split('_')
    if parts:
        interface_id = parts[0]
        if len(interface_id) >= 4:
            return f"GoPro-{interface_id[-4:]}"
    return "Unknown"


def create_aws_signature(method, host, path, headers_to_sign, payload_hash):
    """Create AWS4 signature."""
    now = datetime.datetime.utcnow()
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')

    # Build canonical headers string
    canonical_headers = ""
    signed_header_names = []
    for name, value in sorted(headers_to_sign.items()):
        canonical_headers += f"{name.lower()}:{value}\n"
        signed_header_names.append(name.lower())
    signed_headers = ";".join(signed_header_names)

    # Create canonical request
    canonical_request = f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

    # Create string to sign
    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{AWS_REGION}/s3/aws4_request"
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{canonical_request_hash}"

    # Calculate signature
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    k_date = sign(('AWS4' + AWS_SECRET_KEY).encode('utf-8'), date_stamp)
    k_region = sign(k_date, AWS_REGION)
    k_service = sign(k_region, 's3')
    k_signing = sign(k_service, 'aws4_request')
    signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    # Create authorization header
    authorization = f"{algorithm} Credential={AWS_ACCESS_KEY}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

    return authorization, amz_date


def upload_with_raw_socket(local_path, s3_key, max_retries=5):
    """
    Upload file using raw socket with custom SSL context.
    This gives us more control over the connection parameters.
    """
    file_size = os.path.getsize(local_path)
    host = f"{UPLOAD_BUCKET}.s3.{AWS_REGION}.amazonaws.com"
    path = "/" + quote(s3_key, safe='/')

    # Use UNSIGNED-PAYLOAD for large files
    payload_hash = "UNSIGNED-PAYLOAD"

    for attempt in range(max_retries):
        try:
            # Create custom SSL context with specific settings
            ssl_context = ssl.create_default_context()
            # Try with reduced security for problematic ARM devices
            ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
            ssl_context.options |= ssl.OP_NO_COMPRESSION
            ssl_context.options |= ssl.OP_NO_SSLv2
            ssl_context.options |= ssl.OP_NO_SSLv3

            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)  # 30 second timeout for operations

            # Set socket options for better reliability
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            # Try to set smaller send buffer for more reliable transfers
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
            except:
                pass

            # Connect
            sock.connect((host, 443))

            # Wrap with SSL
            ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host)
            ssl_sock.settimeout(600)  # 10 minute timeout for data transfer

            # Build headers
            headers_to_sign = {
                'Host': host,
                'Content-Type': 'video/mp4',
                'Content-Length': str(file_size),
                'x-amz-content-sha256': payload_hash
            }

            authorization, amz_date = create_aws_signature('PUT', host, path, headers_to_sign, payload_hash)

            # Build HTTP request
            request_lines = [
                f"PUT {path} HTTP/1.1",
                f"Host: {host}",
                f"Content-Type: video/mp4",
                f"Content-Length: {file_size}",
                f"x-amz-date: {amz_date}",
                f"x-amz-content-sha256: {payload_hash}",
                f"Authorization: {authorization}",
                "Connection: close",
                "",
                ""
            ]
            request = "\r\n".join(request_lines)

            # Send headers
            ssl_sock.sendall(request.encode())

            # Send file in chunks
            bytes_sent = 0
            last_progress = 0
            with open(local_path, 'rb') as f:
                while bytes_sent < file_size:
                    chunk = f.read(SEND_CHUNK_SIZE)
                    if not chunk:
                        break

                    # Send chunk in smaller pieces to avoid SSL buffer issues
                    chunk_offset = 0
                    while chunk_offset < len(chunk):
                        try:
                            sent = ssl_sock.send(chunk[chunk_offset:chunk_offset + 16384])
                            if sent == 0:
                                raise Exception("Socket connection broken")
                            chunk_offset += sent
                            bytes_sent += sent
                        except ssl.SSLWantWriteError:
                            # Wait for socket to be ready
                            import select
                            select.select([], [ssl_sock], [], 5.0)
                            continue

                    # Progress update
                    progress = int((bytes_sent / file_size) * 100)
                    if progress >= last_progress + 10:
                        print(f"    {progress}%...")
                        last_progress = progress

            # Read response
            response = b""
            while True:
                try:
                    data = ssl_sock.recv(4096)
                    if not data:
                        break
                    response += data
                    if b"\r\n\r\n" in response:
                        # Got headers, check if there's more
                        if b"Content-Length: 0" in response or response.endswith(b"\r\n\r\n"):
                            break
                except socket.timeout:
                    break

            ssl_sock.close()
            sock.close()

            # Parse response
            response_str = response.decode('utf-8', errors='ignore')
            status_line = response_str.split('\r\n')[0]

            if ' 200 ' in status_line:
                return f"s3://{UPLOAD_BUCKET}/{s3_key}"
            elif ' 500 ' in status_line or ' 503 ' in status_line:
                print(f"    Retry {attempt + 1}/{max_retries} (server error)...")
                import time
                time.sleep(5)
                continue
            else:
                raise Exception(f"Upload failed: {status_line}")

        except ssl.SSLError as e:
            if attempt < max_retries - 1:
                print(f"    SSL error, retry {attempt + 1}/{max_retries}...")
                import time
                time.sleep(5)
                continue
            raise Exception(f"SSL Error: {e}")

        except socket.timeout as e:
            if attempt < max_retries - 1:
                print(f"    Timeout, retry {attempt + 1}/{max_retries}...")
                import time
                time.sleep(5)
                continue
            raise Exception(f"Timeout: {e}")

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"    Error: {e}, retry {attempt + 1}/{max_retries}...")
                import time
                time.sleep(5)
                continue
            raise

        finally:
            try:
                ssl_sock.close()
            except:
                pass
            try:
                sock.close()
            except:
                pass

    raise Exception(f"Upload failed after {max_retries} attempts")


def upload_session(session, delete_after=False):
    """Upload a single session to S3."""
    session_name = session['name']
    session_path = session['path']
    video_files = sorted(session['files'], key=lambda f: f.name)

    upload_date = parse_session_date(session_name)
    if not upload_date:
        print(f"  Could not parse date from session name, skipping")
        return False

    camera_name = extract_camera_name(session_name)

    print(f"  Date: {upload_date}, Camera: {camera_name}")
    print(f"  Files: {len(video_files)}, Size: {session['size_gb']:.2f} GB")

    uploaded = []
    for idx, video_file in enumerate(video_files):
        if len(video_files) > 1:
            file_camera_name = f"{camera_name}_ch{idx+1:02d}"
        else:
            file_camera_name = camera_name

        # Build S3 key
        filename = f"{UPLOAD_DEVICE_NAME} - {file_camera_name}.mp4"
        s3_key = f"{UPLOAD_LOCATION}/{upload_date}/{filename}"

        file_size_mb = video_file.stat().st_size / (1024 * 1024)
        print(f"  Uploading {idx+1}/{len(video_files)}: {video_file.name} ({file_size_mb:.1f} MB)...")

        try:
            s3_uri = upload_with_raw_socket(str(video_file), s3_key)
            print(f"    -> {s3_uri}")
            uploaded.append(video_file)
        except Exception as e:
            print(f"    ERROR: {e}")

    if delete_after and len(uploaded) == len(video_files):
        import shutil
        print(f"  Deleting session folder...")
        shutil.rmtree(session_path)
        print(f"  Deleted.")

    return len(uploaded) == len(video_files)


def main():
    delete_after = "--delete" in sys.argv

    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("ERROR: AWS credentials not found in .env file")
        print("Required: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    print("=" * 60)
    print("GoPro Segment Uploader (raw socket version)")
    print("=" * 60)
    print("This version uses raw sockets with custom SSL settings")
    print("for better control over network parameters.")
    print()
    print(f"Bucket: {UPLOAD_BUCKET}")
    print(f"Location: {UPLOAD_LOCATION}")
    print(f"Device: {UPLOAD_DEVICE_NAME}")
    print(f"Delete after upload: {delete_after}")
    print()

    # Show MTU hint
    print("TIP: If uploads still fail, try reducing MTU:")
    print("     sudo ip link set eth0 mtu 1400")
    print("     # or for WiFi:")
    print("     sudo ip link set wlan0 mtu 1400")
    print()

    sessions = get_sessions()
    if not sessions:
        print("No sessions with video files found.")
        sys.exit(0)

    print(f"Found {len(sessions)} sessions with video files:")
    total_size = sum(s['size_gb'] for s in sessions)
    for s in sessions:
        print(f"  - {s['name']}: {len(s['files'])} files, {s['size_gb']:.2f} GB")
    print(f"Total: {total_size:.2f} GB")
    print()

    response = input("Proceed with upload? [y/N] ").strip().lower()
    if response != 'y':
        print("Aborted.")
        sys.exit(0)

    print()

    success_count = 0
    for i, session in enumerate(sessions):
        print()
        print(f"[{i+1}/{len(sessions)}] {session['name']}")
        if upload_session(session, delete_after):
            success_count += 1

    print()
    print("=" * 60)
    print(f"Upload complete: {success_count}/{len(sessions)} sessions uploaded")
    print("=" * 60)


if __name__ == "__main__":
    main()
