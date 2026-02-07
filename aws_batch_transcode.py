"""
AWS Batch GPU Transcoding Module

Offloads 4K→1080p video transcoding to AWS Batch GPU instances (g4dn.xlarge)
with NVIDIA NVENC hardware encoding for fast processing.

Flow:
1. Jetson extracts 4K clip with stream copy (fast, no encoding)
2. Uploads 4K to S3 raw/ prefix
3. Submits AWS Batch job for GPU transcoding (queue selected by file size)
4. Batch job downloads 4K, encodes to 1080p with h264_nvenc, uploads result
5. Raw 4K file is deleted after successful transcode

Queue Selection (based on 4K file size):
    - gpu-transcode-queue: Files < 14GB (30GB EBS)
    - gpu-transcode-queue-large: Files >= 14GB (100GB EBS)

Encoding Specs (h264_nvenc GPU):
    - Video: h264_nvenc, preset p4, cq 23
    - Scale: scale_cuda=-2:1080 (GPU-accelerated)
    - Audio: AAC 128k
    - movflags: +faststart

Environment Variables:
    USE_AWS_GPU_TRANSCODE: Enable GPU transcoding (true/false)
    AWS_BATCH_JOB_QUEUE: Batch job queue name (default for small files)
    AWS_BATCH_JOB_QUEUE_LARGE: Batch job queue for large files (>=14GB)
    AWS_BATCH_JOB_DEFINITION: Batch job definition name
    AWS_BATCH_REGION: AWS region for Batch (default: us-east-1)
"""

import os
import ssl
import time
from typing import Dict, Any, Optional, List

# Fix SSL issues on Jetson/ARM devices with OpenSSL 3.x (same as videoupload.py)
os.environ.setdefault('OPENSSL_CONF', '/dev/null')

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from logging_service import get_logger

logger = get_logger('gopro.aws_batch_transcode')


class AWSBatchTranscoder:
    """Manages AWS Batch GPU transcoding jobs for video processing."""

    # File size threshold for queue selection (14GB in bytes)
    LARGE_FILE_THRESHOLD = 14 * 1024 * 1024 * 1024  # 14GB

    def __init__(
        self,
        job_queue: str = None,
        job_queue_large: str = None,
        job_definition: str = None,
        region: str = None,
        bucket: str = None
    ):
        """
        Initialize AWS Batch Transcoder.

        Args:
            job_queue: Batch job queue for small files <14GB (default: gpu-transcode-queue)
            job_queue_large: Batch job queue for large files >=14GB (default: gpu-transcode-queue-large)
            job_definition: Batch job definition name (default: ffmpeg-nvenc-transcode:16)
            region: AWS region (default from env: AWS_BATCH_REGION or us-east-1)
            bucket: S3 bucket name (default from env: UPLOAD_BUCKET)
        """
        self.job_queue = job_queue or os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue')
        self.job_queue_large = job_queue_large or os.getenv('AWS_BATCH_JOB_QUEUE_LARGE', 'gpu-transcode-queue-large')
        self.job_definition = job_definition or os.getenv('AWS_BATCH_JOB_DEFINITION', 'ffmpeg-nvenc-transcode:17')
        self.region = region or os.getenv('AWS_BATCH_REGION', os.getenv('AWS_REGION', 'us-east-1'))
        self.bucket = bucket or os.getenv('UPLOAD_BUCKET', 'uball-videos-production')

        # Initialize AWS clients with SSL workarounds for Jetson/ARM OpenSSL 3.x
        boto_config = BotoConfig(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=1,
        )
        self.batch_client = boto3.client('batch', region_name=self.region, config=boto_config, verify=False)
        self.s3_client = boto3.client('s3', region_name=self.region, config=boto_config, verify=False)

        logger.info(f"AWSBatchTranscoder initialized: queue={self.job_queue}, "
                    f"queue_large={self.job_queue_large}, definition={self.job_definition}, "
                    f"region={self.region}, bucket={self.bucket}")

    def select_job_queue(self, file_size_bytes: int) -> str:
        """
        Select the appropriate job queue based on file size.

        Args:
            file_size_bytes: Size of the input file in bytes

        Returns:
            Job queue name (standard for <14GB, large for >=14GB)
        """
        if file_size_bytes >= self.LARGE_FILE_THRESHOLD:
            logger.info(f"File size {file_size_bytes / (1024**3):.2f}GB >= 14GB, using large queue: {self.job_queue_large}")
            return self.job_queue_large
        else:
            logger.info(f"File size {file_size_bytes / (1024**3):.2f}GB < 14GB, using standard queue: {self.job_queue}")
            return self.job_queue

    def submit_transcode_job(
        self,
        input_s3_key: str,
        output_s3_key: str,
        game_id: str,
        angle: str,
        job_name_prefix: str = 'transcode',
        file_size_bytes: int = 0
    ) -> Dict[str, Any]:
        """
        Submit a transcode job to AWS Batch.

        Args:
            input_s3_key: S3 key of 4K source file (e.g., "raw/court-a/2026-01-20/uuid/FL_4k.mp4")
            output_s3_key: S3 key for 1080p output (e.g., "court-a/2026-01-20/uuid/2026-01-20_uuid_FL.mp4")
            game_id: Game identifier for tracking
            angle: Camera angle code (FL, FR, NL, NR)
            job_name_prefix: Prefix for job name
            file_size_bytes: Size of 4K input file in bytes (for queue selection)

        Returns:
            Dict with jobId, jobName, and submission details
        """
        # Generate unique job name
        timestamp = int(time.time())
        job_name = f"{job_name_prefix}-{angle}-{timestamp}"

        # Select queue based on file size
        selected_queue = self.select_job_queue(file_size_bytes)

        # Build S3 URIs
        input_uri = f"s3://{self.bucket}/{input_s3_key}"
        output_uri = f"s3://{self.bucket}/{output_s3_key}"

        logger.info(f"Submitting Batch transcode job: {job_name}")
        logger.info(f"  Queue: {selected_queue}")
        logger.info(f"  Input: {input_uri}")
        logger.info(f"  Output: {output_uri}")

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=selected_queue,
                jobDefinition=self.job_definition,
                parameters={
                    'input_uri': input_uri,
                    'output_uri': output_uri,
                    'bucket': self.bucket,
                    'input_key': input_s3_key,
                    'output_key': output_s3_key
                },
                containerOverrides={
                    'environment': [
                        {'name': 'INPUT_S3_URI', 'value': input_uri},
                        {'name': 'OUTPUT_S3_URI', 'value': output_uri},
                        {'name': 'S3_BUCKET', 'value': self.bucket},
                        {'name': 'INPUT_S3_KEY', 'value': input_s3_key},
                        {'name': 'OUTPUT_S3_KEY', 'value': output_s3_key},
                        {'name': 'GAME_ID', 'value': game_id},
                        {'name': 'ANGLE', 'value': angle}
                    ]
                },
                tags={
                    'game_id': game_id,
                    'angle': angle,
                    'service': 'gopro-automation'
                }
            )

            job_id = response['jobId']
            logger.info(f"Batch job submitted: {job_id}")

            return {
                'jobId': job_id,
                'jobName': job_name,
                'jobQueue': selected_queue,
                'input_s3_key': input_s3_key,
                'output_s3_key': output_s3_key,
                'game_id': game_id,
                'angle': angle,
                'file_size_bytes': file_size_bytes,
                'status': 'SUBMITTED',
                'submitted_at': time.time()
            }

        except ClientError as e:
            logger.error(f"Failed to submit Batch job: {e}")
            raise

    def submit_extract_transcode_job(
        self,
        chapters: List[Dict],
        bucket: str,
        offset_seconds: float,
        duration_seconds: float,
        output_s3_key: str,
        game_id: str,
        angle: str,
        add_buffer_seconds: float = 30.0
    ) -> Dict[str, Any]:
        """
        Submit single Batch job that extracts from chapters AND transcodes to 1080p.
        No Lambda, no intermediate 4K file.

        Args:
            chapters: List of chapter dicts with 's3_key' field
            bucket: S3 bucket name
            offset_seconds: Seek position in concatenated chapters
            duration_seconds: Duration to extract
            output_s3_key: Final 1080p output path (e.g., "court-a/2026-01-20/uuid/video.mp4")
            game_id: Game identifier for tracking
            angle: Camera angle code (FL, FR, NL, NR)
            add_buffer_seconds: Buffer to add to duration (default: 30)

        Returns:
            Dict with jobId, jobName, and submission details
        """
        import json

        # Extract S3 keys from chapters
        chapter_keys = []
        for ch in chapters:
            if isinstance(ch, dict) and 's3_key' in ch:
                chapter_keys.append(ch['s3_key'])
            elif isinstance(ch, str):
                chapter_keys.append(ch)
            else:
                logger.warning(f"Unexpected chapter format: {ch}")

        if not chapter_keys:
            raise ValueError("No valid chapter S3 keys found")

        chapters_json = json.dumps(chapter_keys)

        # Generate unique job name
        timestamp = int(time.time())
        job_name = f"extract-transcode-{angle}-{timestamp}"

        # Get job definition for extract+transcode
        extract_job_definition = os.getenv(
            'AWS_BATCH_JOB_DEFINITION_EXTRACT',
            'ffmpeg-extract-transcode:1'
        )

        # Use large queue for extraction jobs (they process multiple chapters)
        selected_queue = self.job_queue_large

        logger.info(f"[BATCH-ONLY] Submitting extract+transcode job: {job_name}")
        logger.info(f"  Queue: {selected_queue}")
        logger.info(f"  Definition: {extract_job_definition}")
        logger.info(f"  Chapters: {len(chapter_keys)}")
        logger.info(f"  Offset: {offset_seconds}s, Duration: {duration_seconds}s (+{add_buffer_seconds}s buffer)")
        logger.info(f"  Output: s3://{bucket}/{output_s3_key}")

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=selected_queue,
                jobDefinition=extract_job_definition,
                containerOverrides={
                    'environment': [
                        {'name': 'CHAPTERS_JSON', 'value': chapters_json},
                        {'name': 'BUCKET', 'value': bucket},
                        {'name': 'OFFSET_SECONDS', 'value': str(offset_seconds)},
                        {'name': 'DURATION_SECONDS', 'value': str(duration_seconds)},
                        {'name': 'ADD_BUFFER_SECONDS', 'value': str(add_buffer_seconds)},
                        {'name': 'OUTPUT_S3_KEY', 'value': output_s3_key},
                        {'name': 'GAME_ID', 'value': game_id},
                        {'name': 'ANGLE', 'value': angle}
                    ]
                },
                tags={
                    'game_id': game_id,
                    'angle': angle,
                    'service': 'gopro-automation',
                    'pipeline': 'batch-only'
                }
            )

            job_id = response['jobId']
            logger.info(f"[BATCH-ONLY] Job submitted: {job_id}")

            return {
                'job_id': job_id,
                'jobId': job_id,  # Alias for compatibility
                'jobName': job_name,
                'jobQueue': selected_queue,
                'jobDefinition': extract_job_definition,
                'chapters_count': len(chapter_keys),
                'offset_seconds': offset_seconds,
                'duration_seconds': duration_seconds,
                'output_s3_key': output_s3_key,
                'game_id': game_id,
                'angle': angle,
                'status': 'SUBMITTED',
                'submitted_at': time.time(),
                'pipeline': 'batch-only'
            }

        except ClientError as e:
            logger.error(f"[BATCH-ONLY] Failed to submit extract+transcode job: {e}")
            raise

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get status of an AWS Batch job.

        Args:
            job_id: AWS Batch job ID

        Returns:
            Dict with status, statusReason, timestamps, etc.
        """
        try:
            response = self.batch_client.describe_jobs(jobs=[job_id])

            if not response['jobs']:
                return {
                    'jobId': job_id,
                    'status': 'NOT_FOUND',
                    'statusReason': 'Job not found'
                }

            job = response['jobs'][0]

            result = {
                'jobId': job['jobId'],
                'jobName': job.get('jobName', ''),
                'status': job['status'],
                'statusReason': job.get('statusReason', ''),
                'createdAt': job.get('createdAt'),
                'startedAt': job.get('startedAt'),
                'stoppedAt': job.get('stoppedAt')
            }

            # Add container exit code if available
            if 'container' in job:
                container = job['container']
                result['exitCode'] = container.get('exitCode')
                result['reason'] = container.get('reason', '')

            # Add log stream name if available
            if 'container' in job and 'logStreamName' in job['container']:
                result['logStreamName'] = job['container']['logStreamName']

            return result

        except ClientError as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            return {
                'jobId': job_id,
                'status': 'ERROR',
                'statusReason': str(e)
            }

    def wait_for_job(
        self,
        job_id: str,
        timeout: int = 1800,
        poll_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for a Batch job to complete.

        Args:
            job_id: AWS Batch job ID
            timeout: Maximum wait time in seconds (default: 30 minutes)
            poll_interval: Seconds between status checks (default: 30)

        Returns:
            Final job status dict

        Raises:
            TimeoutError: If job doesn't complete within timeout
        """
        terminal_states = {'SUCCEEDED', 'FAILED'}
        start_time = time.time()

        logger.info(f"Waiting for Batch job {job_id} (timeout: {timeout}s)")

        while True:
            status = self.get_job_status(job_id)
            current_status = status.get('status', 'UNKNOWN')

            logger.info(f"  Job {job_id}: {current_status}")

            if current_status in terminal_states:
                return status

            if current_status == 'NOT_FOUND':
                logger.error(f"Job {job_id} not found")
                return status

            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.error(f"Timeout waiting for job {job_id} after {elapsed:.0f}s")
                raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")

            time.sleep(poll_interval)

    def delete_raw_file(self, s3_key: str) -> bool:
        """
        Delete a raw 4K file from S3 after successful transcode.

        Args:
            s3_key: S3 key to delete (e.g., "raw/court-a/2026-01-20/uuid/FL_4k.mp4")

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            logger.info(f"Deleting raw file: s3://{self.bucket}/{s3_key}")

            self.s3_client.delete_object(
                Bucket=self.bucket,
                Key=s3_key
            )

            logger.info(f"Deleted: {s3_key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete {s3_key}: {e}")
            return False

    def get_output_file_info(self, s3_key: str) -> Dict[str, Any]:
        """
        Get information about the transcoded output file.

        Args:
            s3_key: S3 key of output file

        Returns:
            Dict with size_bytes, content_type, last_modified, or error
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket,
                Key=s3_key
            )

            return {
                'exists': True,
                's3_key': s3_key,
                's3_uri': f"s3://{self.bucket}/{s3_key}",
                'size_bytes': response.get('ContentLength', 0),
                'size_mb': round(response.get('ContentLength', 0) / (1024 * 1024), 2),
                'content_type': response.get('ContentType', ''),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None
            }

        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {
                    'exists': False,
                    's3_key': s3_key,
                    'error': 'File not found'
                }
            logger.error(f"Failed to get file info for {s3_key}: {e}")
            return {
                'exists': False,
                's3_key': s3_key,
                'error': str(e)
            }

    def upload_4k_to_raw(
        self,
        local_path: str,
        s3_key: str,
        progress_callback=None
    ) -> Optional[str]:
        """
        Upload a 4K file to the raw/ prefix in S3.

        Args:
            local_path: Path to local 4K file
            s3_key: Target S3 key (should start with "raw/")
            progress_callback: Optional callback(bytes_transferred)

        Returns:
            S3 URI on success, None on failure
        """
        try:
            file_size = os.path.getsize(local_path)
            logger.info(f"Uploading 4K file to S3: {local_path} ({file_size / (1024**3):.2f} GB)")
            logger.info(f"  Target: s3://{self.bucket}/{s3_key}")

            # Use multipart upload for large files
            config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=25 * 1024 * 1024,  # 25 MB
                multipart_chunksize=25 * 1024 * 1024,
                max_concurrency=4
            )

            callback = None
            if progress_callback:
                callback = progress_callback

            self.s3_client.upload_file(
                local_path,
                self.bucket,
                s3_key,
                Config=config,
                Callback=callback
            )

            s3_uri = f"s3://{self.bucket}/{s3_key}"
            logger.info(f"Upload complete: {s3_uri}")
            return s3_uri

        except Exception as e:
            logger.error(f"Failed to upload {local_path} to {s3_key}: {e}")
            return None

    def generate_raw_s3_key(
        self,
        location: str,
        game_date: str,
        uball_game_id: str,
        angle_code: str
    ) -> str:
        """
        Generate S3 key for raw 4K file in raw/ prefix.

        Format: raw/{location}/{date}/{game_folder}/{date}_{game_folder}_{angle}.mp4
        Example: raw/court-a/2026-01-20/3cdc8667-bc9c-4bf6-9050/2026-01-20_3cdc8667-bc9c-4bf6-9050_NL.mp4

        Args:
            location: Court/location identifier (e.g., "court-a")
            game_date: Game date (YYYY-MM-DD)
            uball_game_id: Uball game UUID (game must be synced to Uball first)
            angle_code: Camera angle (FL, FR, NL, NR)

        Returns:
            S3 key string
        """
        # Use first 4 segments of UUID for shorter but still unique folder name
        if uball_game_id:
            uuid_parts = uball_game_id.split('-')[:4]
            folder = '-'.join(uuid_parts)
        else:
            # This shouldn't happen - games should be synced before processing
            folder = f"unknown-{game_date}"

        # Format: {date}_{game_folder}_{angle}.mp4
        filename = f"{game_date}_{folder}_{angle_code}.mp4"
        return f"raw/{location}/{game_date}/{folder}/{filename}"


def is_aws_gpu_transcode_enabled() -> bool:
    """Check if AWS GPU transcoding is enabled via environment variable."""
    return os.getenv('USE_AWS_GPU_TRANSCODE', 'false').lower() == 'true'


def get_batch_transcoder() -> Optional[AWSBatchTranscoder]:
    """
    Get AWSBatchTranscoder instance if GPU transcoding is enabled.

    Returns:
        AWSBatchTranscoder instance or None if not enabled
    """
    if not is_aws_gpu_transcode_enabled():
        return None

    try:
        return AWSBatchTranscoder()
    except Exception as e:
        logger.error(f"Failed to initialize AWSBatchTranscoder: {e}")
        return None
