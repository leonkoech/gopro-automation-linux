"""
Application Configuration
Loads settings from environment variables
"""
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()


class Config:
    """Base configuration"""

    # Flask
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')

    # Storage paths
    VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
    SEGMENTS_DIR = os.path.join(VIDEO_STORAGE_DIR, 'segments')

    # Upload configuration
    UPLOAD_ENABLED = os.getenv('UPLOAD_ENABLED', 'true').lower() == 'true'
    UPLOAD_LOCATION = os.getenv('UPLOAD_LOCATION', 'default-location')
    UPLOAD_DEVICE_NAME = os.getenv('UPLOAD_DEVICE_NAME', os.uname().nodename)
    UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')
    UPLOAD_REGION = os.getenv('UPLOAD_REGION', 'us-east-1')
    DELETE_AFTER_UPLOAD = os.getenv('DELETE_AFTER_UPLOAD', 'false').lower() == 'true'

    # AWS credentials
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

    # AWS Batch configuration
    AWS_BATCH_JOB_QUEUE = os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue')
    AWS_BATCH_JOB_QUEUE_LARGE = os.getenv('AWS_BATCH_JOB_QUEUE_LARGE', 'gpu-transcode-queue-large')
    AWS_BATCH_JOB_DEFINITION = os.getenv('AWS_BATCH_JOB_DEFINITION', 'ffmpeg-nvenc-transcode:17')
    AWS_BATCH_JOB_DEFINITION_EXTRACT = os.getenv('AWS_BATCH_JOB_DEFINITION_EXTRACT', 'ffmpeg-extract-transcode:3')
    AWS_BATCH_REGION = os.getenv('AWS_BATCH_REGION', 'us-east-1')

    # Court location
    COURT_LOCATION = os.getenv('COURT_LOCATION', 'court-a')

    # Firebase
    FIREBASE_CREDENTIALS_PATH = os.getenv('FIREBASE_CREDENTIALS_PATH')
    JETSON_ID = os.getenv('JETSON_ID', 'unknown')
    CAMERA_ANGLE_MAP = os.getenv('CAMERA_ANGLE_MAP', '{}')

    # Uball Backend
    UBALL_BACKEND_URL = os.getenv('UBALL_BACKEND_URL')
    UBALL_AUTH_EMAIL = os.getenv('UBALL_AUTH_EMAIL')
    UBALL_AUTH_PASSWORD = os.getenv('UBALL_AUTH_PASSWORD')

    # Download configuration - optimized for GoPro USB connections
    DOWNLOAD_CHUNK_SIZE = 262144  # 256KB
    DOWNLOAD_CONNECT_TIMEOUT = 10  # seconds
    DOWNLOAD_READ_TIMEOUT = 60  # seconds
    DOWNLOAD_MAX_RETRIES = 20
    DOWNLOAD_KEEP_ALIVE_INTERVAL = 30  # seconds

    @classmethod
    def ensure_directories(cls):
        """Create necessary directories"""
        os.makedirs(cls.VIDEO_STORAGE_DIR, exist_ok=True)
        os.makedirs(cls.SEGMENTS_DIR, exist_ok=True)


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True


class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False


# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': ProductionConfig
}


def get_config():
    """Get configuration based on environment"""
    env = os.getenv('FLASK_ENV', 'production')
    return config.get(env, config['default'])
