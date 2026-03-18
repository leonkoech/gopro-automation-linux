"""
Lambda function for extracting game clips from S3 video chapters.

This Lambda reads video chapters directly from S3 using presigned URLs,
extracts the game portion using FFmpeg, uploads to S3, and optionally
submits an AWS Batch job for GPU transcoding.

Flow:
1. Read chapters from S3 via presigned URLs (HTTPS)
2. Extract game clip using FFmpeg (stream copy or compress)
3. Upload extracted clip to S3
4. If batch_config provided: Submit AWS Batch transcode job

Input event:
{
    "chapters": [
        {"s3_key": "raw-chapters/session123/chapter_001.MP4", "duration_seconds": 1800}
    ],
    "bucket": "uball-videos-production",
    "offset_seconds": 300.0,
    "duration_seconds": 2400.0,
    "output_s3_key": "raw/court-a/2026-01-20/game-uuid/FL.mp4",
    "compress_to_1080p": false,
    "add_buffer_seconds": 30.0,

    // Optional: Submit Batch job after extraction
    "batch_config": {
        "job_queue": "gpu-transcode-queue",
        "job_definition": "ffmpeg-nvenc-transcode:17",
        "final_s3_key": "court-a/2026-01-20/game-uuid/FL.mp4",
        "game_id": "firebase_game_id",
        "angle": "FL"
    }
}

Output:
{
    "success": true,
    "output_s3_uri": "s3://bucket/raw/...",
    "output_size_bytes": 1234567890,
    "processing_time_seconds": 120.5,
    "batch_job": {  // Only if batch_config provided
        "job_id": "abc-123",
        "job_name": "transcode-FL-...",
        "status": "SUBMITTED"
    },
    "error": null
}
"""

import json
import os
import subprocess
import tempfile
import time
import boto3
from typing import Dict, Any, List, Optional


# Initialize AWS clients
s3_client = boto3.client('s3')
batch_client = boto3.client('batch')

# FFmpeg binary path (use Lambda layer or bundled binary)
FFMPEG_PATH = os.environ.get('FFMPEG_PATH', '/opt/bin/ffmpeg')
FFPROBE_PATH = os.environ.get('FFPROBE_PATH', '/opt/bin/ffprobe')


def handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler for video extraction and optional Batch submission.
    """
    start_time = time.time()

    # Parse input
    chapters = event.get('chapters', [])
    bucket = event.get('bucket', 'uball-videos-production')
    offset_seconds = float(event.get('offset_seconds', 0))
    duration_seconds = float(event.get('duration_seconds', 0))
    output_s3_key = event.get('output_s3_key', '')
    compress_to_1080p = event.get('compress_to_1080p', True)
    add_buffer = float(event.get('add_buffer_seconds', 30.0))
    batch_config = event.get('batch_config')  # Optional

    # Validate input
    if not chapters:
        return error_response("No chapters provided")
    if not output_s3_key:
        return error_response("No output_s3_key provided")
    if duration_seconds <= 0:
        return error_response("Invalid duration_seconds")

    print(f"Extracting {len(chapters)} chapters")
    print(f"  Offset: {offset_seconds}s, Duration: {duration_seconds}s")
    print(f"  Output: s3://{bucket}/{output_s3_key}")
    print(f"  Compress to 1080p: {compress_to_1080p}")
    if batch_config:
        print(f"  Will submit Batch job after extraction")

    # Apply buffer (but don't go negative)
    buffered_offset = max(0, offset_seconds - add_buffer)
    buffered_duration = duration_seconds + (2 * add_buffer)

    try:
        # Generate presigned URLs for each chapter (valid for 2 hours)
        chapter_urls = []
        for chapter in chapters:
            s3_key = chapter.get('s3_key')
            if not s3_key:
                return error_response(f"Chapter missing s3_key: {chapter}")

            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': s3_key},
                ExpiresIn=7200
            )
            chapter_urls.append(url)
            print(f"  Chapter: {s3_key}")

        # Create temp directory for work
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'output.mp4')

            # Build FFmpeg command
            if len(chapter_urls) == 1:
                cmd = build_single_file_cmd(
                    chapter_urls[0],
                    buffered_offset,
                    buffered_duration,
                    output_path,
                    compress_to_1080p
                )
            else:
                concat_file = os.path.join(temp_dir, 'concat.txt')
                with open(concat_file, 'w') as f:
                    for url in chapter_urls:
                        escaped_url = url.replace("'", "'\\''")
                        f.write(f"file '{escaped_url}'\n")

                cmd = build_concat_cmd(
                    concat_file,
                    buffered_offset,
                    buffered_duration,
                    output_path,
                    compress_to_1080p
                )

            print(f"Running FFmpeg...")

            # Run FFmpeg with timeout (13 minutes to leave buffer for upload + batch)
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=780  # 13 minutes
            )

            if result.returncode != 0:
                print(f"FFmpeg stderr: {result.stderr}")
                return error_response(f"FFmpeg failed: {result.stderr[-500:]}")

            if not os.path.exists(output_path):
                return error_response("FFmpeg completed but output file not found")

            output_size = os.path.getsize(output_path)
            print(f"Extraction complete: {output_size / (1024*1024):.1f} MB")

            # Upload to S3
            print(f"Uploading to s3://{bucket}/{output_s3_key}")
            s3_client.upload_file(
                output_path,
                bucket,
                output_s3_key,
                ExtraArgs={'ContentType': 'video/mp4'}
            )

            processing_time = time.time() - start_time
            print(f"Extraction + upload done in {processing_time:.1f}s")

            # Build response
            response = {
                'success': True,
                'output_s3_uri': f"s3://{bucket}/{output_s3_key}",
                'output_size_bytes': output_size,
                'processing_time_seconds': round(processing_time, 1),
                'error': None
            }

            # Submit Batch job if config provided
            if batch_config:
                batch_result = submit_batch_job(
                    bucket=bucket,
                    input_s3_key=output_s3_key,
                    output_s3_key=batch_config.get('final_s3_key'),
                    job_queue=batch_config.get('job_queue', 'gpu-transcode-queue'),
                    job_definition=batch_config.get('job_definition', 'ffmpeg-nvenc-transcode:17'),
                    game_id=batch_config.get('game_id', 'unknown'),
                    angle=batch_config.get('angle', 'UNK'),
                    file_size_bytes=output_size
                )
                response['batch_job'] = batch_result
                print(f"Batch job submitted: {batch_result.get('job_id')}")

            print(f"Total time: {time.time() - start_time:.1f}s")
            return response

    except subprocess.TimeoutExpired:
        return error_response("FFmpeg timed out (>13 minutes)")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return error_response(str(e))


def submit_batch_job(
    bucket: str,
    input_s3_key: str,
    output_s3_key: str,
    job_queue: str,
    job_definition: str,
    game_id: str,
    angle: str,
    file_size_bytes: int
) -> Dict[str, Any]:
    """
    Submit AWS Batch GPU transcode job.
    """
    # Select queue based on file size (14GB threshold)
    LARGE_FILE_THRESHOLD = 14 * 1024 * 1024 * 1024
    if file_size_bytes >= LARGE_FILE_THRESHOLD:
        # Use large queue for files >= 14GB
        if 'large' not in job_queue.lower():
            job_queue = job_queue.replace('queue', 'queue-large')

    job_name = f"transcode-{angle}-{game_id[:8]}-{int(time.time())}"

    input_s3_uri = f"s3://{bucket}/{input_s3_key}"
    output_s3_uri = f"s3://{bucket}/{output_s3_key}"

    try:
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            containerOverrides={
                'environment': [
                    {'name': 'INPUT_S3_URI', 'value': input_s3_uri},
                    {'name': 'OUTPUT_S3_URI', 'value': output_s3_uri},
                    {'name': 'GAME_ID', 'value': game_id},
                    {'name': 'ANGLE', 'value': angle},
                ]
            }
        )

        return {
            'job_id': response['jobId'],
            'job_name': response['jobName'],
            'job_queue': job_queue,
            'status': 'SUBMITTED',
            'input_s3_uri': input_s3_uri,
            'output_s3_uri': output_s3_uri
        }

    except Exception as e:
        print(f"Batch submit error: {e}")
        return {
            'job_id': None,
            'job_name': job_name,
            'status': 'FAILED',
            'error': str(e)
        }


def build_single_file_cmd(
    input_url: str,
    offset: float,
    duration: float,
    output_path: str,
    compress: bool
) -> List[str]:
    """Build FFmpeg command for single file extraction."""
    cmd = [
        FFMPEG_PATH, '-y',
        '-ss', str(offset),
        '-i', input_url,
        '-t', str(duration),
    ]

    if compress:
        cmd.extend([
            '-vf', 'scale=-2:1080',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
        ])
    else:
        cmd.extend(['-c', 'copy'])

    cmd.extend(['-avoid_negative_ts', 'make_zero', output_path])
    return cmd


def build_concat_cmd(
    concat_file: str,
    offset: float,
    duration: float,
    output_path: str,
    compress: bool
) -> List[str]:
    """Build FFmpeg command for multi-file concat extraction."""
    cmd = [
        FFMPEG_PATH, '-y',
        '-ss', str(offset),
        '-f', 'concat',
        '-safe', '0',
        '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
        '-i', concat_file,
        '-t', str(duration),
    ]

    if compress:
        cmd.extend([
            '-vf', 'scale=-2:1080',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-crf', '23',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
        ])
    else:
        cmd.extend(['-c', 'copy'])

    cmd.extend(['-avoid_negative_ts', 'make_zero', output_path])
    return cmd


def error_response(message: str) -> Dict[str, Any]:
    """Return error response."""
    return {
        'success': False,
        'output_s3_uri': None,
        'output_size_bytes': 0,
        'processing_time_seconds': 0,
        'batch_job': None,
        'error': message
    }


# For local testing
if __name__ == '__main__':
    test_event = {
        "chapters": [
            {"s3_key": "raw-chapters/test/chapter_001.MP4", "duration_seconds": 1800}
        ],
        "bucket": "uball-videos-production",
        "offset_seconds": 60.0,
        "duration_seconds": 300.0,
        "output_s3_key": "test/output.mp4",
        "compress_to_1080p": False,
        "add_buffer_seconds": 10.0,
        "batch_config": {
            "job_queue": "gpu-transcode-queue",
            "job_definition": "ffmpeg-nvenc-transcode:17",
            "final_s3_key": "test/final.mp4",
            "game_id": "test-game-123",
            "angle": "FL"
        }
    }

    result = handler(test_event, None)
    print(json.dumps(result, indent=2))
