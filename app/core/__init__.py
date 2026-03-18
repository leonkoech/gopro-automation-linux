"""
Core Application State and Utilities

This module contains shared state that needs to be accessed across blueprints.
"""
import threading
import uuid

# Global recording state
recording_processes = {}  # gopro_id -> recording state dict
recording_lock = threading.Lock()
gopro_ip_cache = {}  # interface -> gopro_ip

# Video processing jobs (async)
video_processing_jobs = {}  # job_id -> job_state
video_processing_lock = threading.Lock()

# Pipeline jobs
pipeline_jobs = {}  # job_id -> job_state
pipeline_jobs_lock = threading.Lock()

# Admin jobs
admin_jobs = {}  # job_id -> job_state
admin_jobs_lock = threading.Lock()

# Upload jobs
upload_jobs = {}  # upload_id -> job_state
upload_jobs_lock = threading.Lock()


def generate_job_id():
    """Generate a unique job ID"""
    return str(uuid.uuid4())[:8]
