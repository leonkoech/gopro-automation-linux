"""
Background 4K recording uploader — download from Z-CAM, upload to S3.

Runs a worker thread consuming from a queue. The API poller enqueues
files when a recording ends. The upload NEVER blocks the live pipeline.

On Orange Pi, the worker thread runs at nice(10) to leave CPU headroom
for the live GStreamer encode.

S3 upload pattern mirrors s3_upload_helper.py and videoupload.py.
"""

import os
import shutil
import threading
import queue
import logging
import time
import requests
import boto3
from botocore.config import Config as BotoConfig
from datetime import datetime
from typing import Dict, Optional

from ..config import ZCamConfig, ZCamCamera

logger = logging.getLogger('zcam.uploader')


class UploadJob:
    """Tracks a single 4K file download + S3 upload."""

    def __init__(self, camera: ZCamCamera, file_info: dict):
        self.camera = camera
        self.file_info = file_info  # {folder, filename, url, cam_id}
        self.id = f"{camera.cam_id}_{file_info['filename']}"
        self.status = 'queued'  # queued | downloading | uploading | completed | failed
        self.progress = 0.0
        self.error: Optional[str] = None
        self.s3_key: Optional[str] = None
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        self.total_bytes = 0


class ZCamUploader:
    """Background upload queue for 4K recordings."""

    PART_SIZE = 25 * 1024 * 1024  # 25MB multipart parts
    MIN_FREE_SPACE_GB = 5  # Skip download if less than this free

    def __init__(self, config: ZCamConfig):
        self.config = config
        self._queue: queue.Queue = queue.Queue()
        self._jobs: Dict[str, UploadJob] = {}
        self._jobs_lock = threading.Lock()
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        os.makedirs(config.download_dir, exist_ok=True)

        # S3 client with retry config
        self._s3 = boto3.client(
            's3',
            region_name=config.s3_region,
            config=BotoConfig(
                retries={'max_attempts': 10, 'mode': 'adaptive'},
                connect_timeout=60,
                read_timeout=300,
            ),
        )

    def start(self):
        """Start the upload worker thread."""
        self._worker_thread = threading.Thread(
            target=self._worker, daemon=True, name='zcam-uploader'
        )
        self._worker_thread.start()
        logger.info("Z-CAM uploader started")

    def stop(self):
        """Stop the upload worker (finishes current job)."""
        self._stop_event.set()
        self._queue.put(None)  # Sentinel to unblock .get()
        if self._worker_thread:
            self._worker_thread.join(timeout=30)
        logger.info("Z-CAM uploader stopped")

    def enqueue(self, camera: ZCamCamera, file_info: dict):
        """Add a file to the upload queue."""
        job = UploadJob(camera, file_info)
        with self._jobs_lock:
            # Skip if already queued/in-progress
            if job.id in self._jobs and self._jobs[job.id].status in ('queued', 'downloading', 'uploading'):
                logger.info(f"Upload already in progress: {job.id}")
                return
            self._jobs[job.id] = job
        self._queue.put(job)
        logger.info(f"Queued upload: {job.id}")

    def _worker(self):
        """Process upload jobs from the queue."""
        # On Orange Pi, reduce CPU priority
        if self.config.platform == 'orangepi':
            try:
                os.nice(10)
                logger.info("Upload worker running at nice +10 (Orange Pi)")
            except (OSError, AttributeError):
                pass  # nice() not available on Windows / permission denied

        while not self._stop_event.is_set():
            try:
                job = self._queue.get(timeout=2)
                if job is None:
                    break
                self._process_job(job)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)

    def _process_job(self, job: UploadJob):
        """Download from Z-CAM, then upload to S3."""
        job.started_at = time.time()
        local_path = os.path.join(
            self.config.download_dir,
            f"{job.camera.cam_id}_{job.file_info['filename']}"
        )

        try:
            # Check disk space
            if not self._check_disk_space():
                job.status = 'failed'
                job.error = f'Insufficient disk space (< {self.MIN_FREE_SPACE_GB}GB free)'
                logger.warning(f"Skipping {job.id}: {job.error}")
                return

            # Phase 1: Download from Z-CAM
            job.status = 'downloading'
            self._download_file(job, local_path)

            # Phase 2: Upload to S3
            job.status = 'uploading'
            date_str = datetime.now().strftime('%Y-%m-%d')
            s3_key = (
                f"{self.config.s3_prefix}/{date_str}/"
                f"{job.camera.cam_id}/{job.file_info['filename']}"
            )
            self._upload_to_s3(job, local_path, s3_key)

            job.status = 'completed'
            job.s3_key = s3_key
            job.completed_at = time.time()
            elapsed = round(job.completed_at - job.started_at, 1)
            logger.info(
                f"Upload complete: {job.id} → s3://{self.config.s3_bucket}/{s3_key} "
                f"({elapsed}s)"
            )

        except Exception as e:
            job.status = 'failed'
            job.error = str(e)
            logger.error(f"Upload failed for {job.id}: {e}")
        finally:
            # Clean up temp file
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass

    def _check_disk_space(self) -> bool:
        """Check if download dir has enough free space."""
        try:
            usage = shutil.disk_usage(self.config.download_dir)
            free_gb = usage.free / (1024 ** 3)
            if free_gb < self.MIN_FREE_SPACE_GB:
                logger.warning(
                    f"Low disk space: {free_gb:.1f}GB free "
                    f"(need {self.MIN_FREE_SPACE_GB}GB)"
                )
                return False
            return True
        except Exception:
            return True  # If we can't check, try anyway

    def _download_file(self, job: UploadJob, local_path: str):
        """Stream-download from Z-CAM HTTP to local file."""
        url = job.file_info['url']
        logger.info(f"Downloading {job.id} from {url}")

        with requests.get(url, stream=True, timeout=(10, 60)) as r:
            r.raise_for_status()
            job.total_bytes = int(r.headers.get('content-length', 0))

            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=self.config.chunk_size):
                    if self._stop_event.is_set():
                        raise RuntimeError("Upload cancelled — shutting down")
                    f.write(chunk)
                    job.bytes_downloaded += len(chunk)
                    if job.total_bytes > 0:
                        job.progress = (job.bytes_downloaded / job.total_bytes) * 50  # 0-50%

        logger.info(f"Download complete: {job.id} ({job.bytes_downloaded} bytes)")

    def _upload_to_s3(self, job: UploadJob, local_path: str, s3_key: str):
        """Multipart upload to S3."""
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading {job.id} to s3://{self.config.s3_bucket}/{s3_key} ({file_size} bytes)")

        # Simple upload for small files
        if file_size <= self.PART_SIZE * 2:
            self._s3.upload_file(local_path, self.config.s3_bucket, s3_key)
            job.bytes_uploaded = file_size
            job.progress = 100
            return

        # Multipart upload for large files
        mpu = self._s3.create_multipart_upload(
            Bucket=self.config.s3_bucket, Key=s3_key
        )
        upload_id = mpu['UploadId']
        parts = []

        try:
            with open(local_path, 'rb') as f:
                part_number = 1
                while True:
                    data = f.read(self.PART_SIZE)
                    if not data:
                        break
                    if self._stop_event.is_set():
                        raise RuntimeError("Upload cancelled — shutting down")

                    resp = self._s3.upload_part(
                        Bucket=self.config.s3_bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=data,
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': resp['ETag'],
                    })
                    job.bytes_uploaded += len(data)
                    job.progress = 50 + (job.bytes_uploaded / file_size) * 50  # 50-100%
                    part_number += 1

            self._s3.complete_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
            )
        except Exception:
            # Abort the multipart upload to avoid S3 charges for incomplete parts
            self._s3.abort_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
            )
            raise

    def get_all_jobs(self) -> list:
        """Return all upload jobs as dicts."""
        with self._jobs_lock:
            return [self._job_to_dict(j) for j in self._jobs.values()]

    def get_job(self, job_id: str) -> Optional[dict]:
        """Return a single upload job as dict."""
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            return self._job_to_dict(job) if job else None

    def _job_to_dict(self, job: UploadJob) -> dict:
        return {
            'id': job.id,
            'cam_id': job.camera.cam_id,
            'filename': job.file_info['filename'],
            'status': job.status,
            'progress': round(job.progress, 1),
            'bytes_downloaded': job.bytes_downloaded,
            'bytes_uploaded': job.bytes_uploaded,
            'total_bytes': job.total_bytes,
            's3_key': job.s3_key,
            'error': job.error,
        }
