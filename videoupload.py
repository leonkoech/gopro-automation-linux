"""
Video Upload Service for S3
Compresses videos to 1080p and uploads to S3 with organized folder structure.

Requirements:
    pip install boto3 python-dotenv
    sudo apt install ffmpeg
"""

import os
import subprocess
import tempfile
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import boto3
from boto3.s3.transfer import TransferConfig

# Load environment variables from .env file
load_dotenv()
from botocore.exceptions import ClientError
from botocore.config import Config as BotoConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VideoUploadService:
    """
    Service for compressing and uploading videos to S3.
    
    Folder structure in S3:
        {location}/{date}/{device_name} - {camera_name}.mp4
    """
    
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str = "jetson-videos-uai",
        region: str = "us-east-1"
    ):
        """
        Initialize the video upload service.
        
        Args:
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            bucket_name: S3 bucket name
            region: AWS region
        """
        self.bucket_name = bucket_name
        self.region = region

        # Configure boto with retries and timeouts for Jetson devices
        boto_config = BotoConfig(
            retries={
                'max_attempts': 5,
                'mode': 'adaptive'
            },
            connect_timeout=30,
            read_timeout=60,
            max_pool_connections=10
        )

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
            config=boto_config
        )

        # Transfer config for Jetson/ARM devices with SSL issues
        # Use single-threaded uploads to avoid SSL EOF errors
        self.transfer_config = TransferConfig(
            multipart_threshold=50 * 1024 * 1024,  # 50MB - only use multipart for larger files
            max_concurrency=1,  # Single thread to avoid SSL issues on ARM
            multipart_chunksize=50 * 1024 * 1024,  # 50MB chunks - fewer parts = fewer SSL connections
            use_threads=False  # Disable threading entirely
        )

        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self) -> None:
        """Create the S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                logger.info(f"Creating bucket '{self.bucket_name}'")
                if self.region == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                logger.info(f"Bucket '{self.bucket_name}' created successfully")
            else:
                raise
    
    def compress_to_1080p(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        crf: int = 18
    ) -> str:
        """
        Compress video to 1080p using FFmpeg with high quality settings.
        
        Args:
            input_path: Path to the input video file
            output_path: Path for the output file (auto-generated if None)
            crf: Constant Rate Factor (0-51, lower = better quality, 18 is visually lossless)
        
        Returns:
            Path to the compressed video file
        """
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input video not found: {input_path}")
        
        if output_path is None:
            temp_dir = tempfile.mkdtemp()
            output_path = os.path.join(temp_dir, "compressed_1080p.mp4")
        
        # FFmpeg command for high-quality 1080p compression
        # Using libx264 with CRF 18 (visually lossless)
        # preset 'slow' gives better compression at the cost of encoding time
        cmd = [
            'ffmpeg',
            '-i', input_path,
            '-vf', 'scale=-2:1080',  # Scale to 1080p height, auto-calculate width (divisible by 2)
            '-c:v', 'libx264',        # H.264 codec
            '-preset', 'slow',        # Slower preset = better quality/compression ratio
            '-crf', str(crf),         # Quality level (18 = visually lossless)
            '-c:a', 'aac',            # AAC audio codec
            '-b:a', '192k',           # Audio bitrate
            '-movflags', '+faststart', # Enable streaming
            '-y',                      # Overwrite output file
            output_path
        ]
        
        logger.info(f"Compressing video: {input_path}")
        logger.info(f"FFmpeg command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"Video compressed successfully: {output_path}")
            return output_path
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error: {e.stderr}")
            raise RuntimeError(f"Video compression failed: {e.stderr}")
    
    def _build_s3_key(
        self,
        location: str,
        date: str,
        device_name: str,
        camera_name: str
    ) -> str:
        """
        Build the S3 object key based on the folder structure.
        
        Args:
            location: Location identifier
            date: Date string (e.g., "2025-12-20")
            device_name: Device name
            camera_name: Camera name
        
        Returns:
            S3 object key
        """
        # Sanitize inputs to avoid path issues
        location = location.strip().replace('/', '-')
        date = date.strip().replace('/', '-')
        device_name = device_name.strip().replace('/', '-')
        camera_name = camera_name.strip().replace('/', '-')
        
        filename = f"{device_name} - {camera_name}.mp4"
        return f"{location}/{date}/{filename}"
    
    def upload_video(
        self,
        video_path: str,
        location: str,
        date: str,
        device_name: str,
        camera_name: str,
        compress: bool = True,
        delete_compressed_after_upload: bool = True
    ) -> str:
        """
        Compress (optionally) and upload a video to S3.
        
        Args:
            video_path: Path to the video file
            location: Location identifier for folder structure
            date: Date string for folder structure
            device_name: Device name for filename
            camera_name: Camera name for filename
            compress: Whether to compress to 1080p before uploading
            delete_compressed_after_upload: Delete temp compressed file after upload
        
        Returns:
            S3 URI of the uploaded video
        """
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video not found: {video_path}")
        
        # Compress if requested
        if compress:
            upload_path = self.compress_to_1080p(video_path)
        else:
            upload_path = video_path
        
        # Build S3 key
        s3_key = self._build_s3_key(location, date, device_name, camera_name)
        
        try:
            # Upload with progress callback for large files
            file_size = os.path.getsize(upload_path)
            logger.info(f"Uploading {upload_path} ({file_size / 1024 / 1024:.2f} MB) to s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.upload_file(
                upload_path,
                self.bucket_name,
                s3_key,
                Callback=ProgressCallback(file_size) if file_size > 10 * 1024 * 1024 else None,
                ExtraArgs={'ContentType': 'video/mp4'},
                Config=self.transfer_config
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Upload complete: {s3_uri}")
            
            return s3_uri
            
        finally:
            # Clean up compressed file if it was created
            if compress and delete_compressed_after_upload and upload_path != video_path:
                try:
                    os.remove(upload_path)
                    # Also try to remove the temp directory
                    os.rmdir(os.path.dirname(upload_path))
                except OSError:
                    pass
    
    def list_videos(self, location: Optional[str] = None, date: Optional[str] = None) -> list:
        """
        List videos in the bucket, optionally filtered by location and/or date.

        Args:
            location: Filter by location (optional)
            date: Filter by date (optional)

        Returns:
            List of S3 object keys
        """
        prefix = ""
        if location:
            prefix = f"{location}/"
            if date:
                prefix = f"{location}/{date}/"

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' not in response:
                return []

            return [obj['Key'] for obj in response['Contents']]

        except ClientError as e:
            logger.error(f"Error listing videos: {e}")
            raise

    def list_videos_with_metadata(self, location: Optional[str] = None, date: Optional[str] = None) -> list:
        """
        List videos in the bucket with full metadata.

        Args:
            location: Filter by location (optional)
            date: Filter by date (optional)

        Returns:
            List of video objects with metadata
        """
        prefix = ""
        if location:
            prefix = f"{location}/"
            if date:
                prefix = f"{location}/{date}/"

        try:
            videos = []
            paginator = self.s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    key = obj['Key']
                    # Skip non-video files
                    if not key.lower().endswith('.mp4'):
                        continue

                    # Parse the key to extract location, date, device, camera
                    parts = key.split('/')
                    if len(parts) >= 3:
                        vid_location = parts[0]
                        vid_date = parts[1]
                        filename = parts[2]

                        # Parse filename: "device_name - camera_name.mp4"
                        name_part = filename.replace('.mp4', '')
                        if ' - ' in name_part:
                            device_name, camera_name = name_part.split(' - ', 1)
                        else:
                            device_name = name_part
                            camera_name = 'Unknown'
                    else:
                        vid_location = 'Unknown'
                        vid_date = 'Unknown'
                        device_name = 'Unknown'
                        camera_name = 'Unknown'
                        filename = key.split('/')[-1]

                    videos.append({
                        'key': key,
                        'filename': filename,
                        'location': vid_location,
                        'date': vid_date,
                        'device_name': device_name,
                        'camera_name': camera_name,
                        'size_bytes': obj['Size'],
                        'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag': obj['ETag'].strip('"')
                    })

            # Sort by last modified, newest first
            videos.sort(key=lambda x: x['last_modified'], reverse=True)
            return videos

        except ClientError as e:
            logger.error(f"Error listing videos: {e}")
            raise

    def get_presigned_url(self, s3_key: str, expiration: int = 3600) -> str:
        """
        Generate a presigned URL for streaming/downloading a video.

        Args:
            s3_key: The S3 object key
            expiration: URL expiration time in seconds (default 1 hour)

        Returns:
            Presigned URL string
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {e}")
            raise

    def delete_video(self, s3_key: str) -> bool:
        """
        Delete a video from S3.

        Args:
            s3_key: The S3 object key

        Returns:
            True if successful
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted video: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting video: {e}")
            raise

    def get_unique_locations(self) -> list:
        """Get list of unique locations in the bucket."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )

            locations = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    location = prefix['Prefix'].rstrip('/')
                    locations.append(location)

            return sorted(locations)
        except ClientError as e:
            logger.error(f"Error listing locations: {e}")
            raise

    def get_dates_for_location(self, location: str) -> list:
        """Get list of dates for a specific location."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"{location}/",
                Delimiter='/'
            )

            dates = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    # Extract date from "location/date/"
                    date = prefix['Prefix'].split('/')[-2]
                    dates.append(date)

            return sorted(dates, reverse=True)
        except ClientError as e:
            logger.error(f"Error listing dates: {e}")
            raise


class ProgressCallback:
    """Callback class to track upload progress."""
    
    def __init__(self, total_size: int):
        self.total_size = total_size
        self.uploaded = 0
        self.last_percentage = 0
    
    def __call__(self, bytes_amount: int):
        self.uploaded += bytes_amount
        percentage = int((self.uploaded / self.total_size) * 100)
        
        # Only log every 10%
        if percentage >= self.last_percentage + 10:
            self.last_percentage = percentage
            logger.info(f"Upload progress: {percentage}%")


# Example usage
if __name__ == "__main__":
    # Credentials loaded from .env file
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise ValueError("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env file")
    
    # Initialize the service
    service = VideoUploadService(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        bucket_name="jetson-videos",
        region="us-east-1"  # Change to your preferred region
    )
    
    # Upload a video
    s3_uri = service.upload_video(
        video_path="/path/to/your/video.mp4",
        location="warehouse-01",
        date="2025-12-20",
        device_name="Jetson-Nano-001",
        camera_name="Front-Door"
    )
    
    print(f"Video uploaded to: {s3_uri}")
    # Result: s3://jetson-videos/warehouse-01/2025-12-20/Jetson-Nano-001 - Front-Door.mp4
    
    # List all videos
    videos = service.list_videos()
    print(f"All videos: {videos}")
    
    # List videos for a specific location and date
    videos = service.list_videos(location="warehouse-01", date="2025-12-20")
    print(f"Filtered videos: {videos}")