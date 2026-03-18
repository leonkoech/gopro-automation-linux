"""
GoPro Controller Flask Application Factory
"""
# Fix SSL issues on Jetson/ARM devices - MUST be set before any SSL imports
import os
os.environ['OPENSSL_CONF'] = '/dev/null'

from flask import Flask
from flask_cors import CORS

from .config import get_config


def create_app(config_class=None):
    """
    Application factory for creating Flask app instance.

    Args:
        config_class: Configuration class to use (optional)

    Returns:
        Flask application instance
    """
    app = Flask(__name__)

    # Load configuration
    if config_class is None:
        config_class = get_config()
    app.config.from_object(config_class)

    # Enable CORS
    CORS(app, resources={
        r"/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })

    # Ensure directories exist
    config_class.ensure_directories()

    # Initialize services
    _init_services(app)

    # Register blueprints
    _register_blueprints(app)

    return app


def _init_services(app):
    """Initialize application services and store in app context"""
    from logging_service import get_logging_service, get_logger
    from firebase_service import get_firebase_service
    from uball_client import get_uball_client
    from videoupload import VideoUploadService
    from video_processing import VideoProcessor

    # Initialize logging first
    logging_service = get_logging_service()
    logger = get_logger('gopro.app')

    # Initialize upload service
    upload_service = None
    if app.config.get('UPLOAD_ENABLED'):
        aws_access_key = app.config.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = app.config.get('AWS_SECRET_ACCESS_KEY')
        if aws_access_key and aws_secret_key:
            try:
                upload_service = VideoUploadService(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    bucket_name=app.config.get('UPLOAD_BUCKET'),
                    region=app.config.get('UPLOAD_REGION')
                )
                logger.info(f"Video upload service initialized (bucket: {app.config.get('UPLOAD_BUCKET')})")
            except Exception as e:
                logger.warning(f"Failed to initialize upload service: {e}")
        else:
            logger.warning("Upload enabled but AWS credentials not found")

    # Initialize Firebase service
    firebase_service = get_firebase_service()
    if firebase_service:
        logger.info(f"Firebase service initialized (Jetson ID: {firebase_service.jetson_id})")
    else:
        logger.warning("Firebase service not available")

    # Initialize Uball client
    uball_client = get_uball_client()
    if uball_client:
        logger.info("Uball Backend client initialized")
    else:
        logger.warning("Uball Backend client not available")

    # Initialize Video Processor
    video_processor = VideoProcessor(
        app.config.get('VIDEO_STORAGE_DIR'),
        app.config.get('SEGMENTS_DIR')
    )
    logger.info(f"Video processor initialized")

    # Store services in app extensions
    app.extensions['services'] = {
        'logging': logging_service,
        'upload': upload_service,
        'firebase': firebase_service,
        'uball': uball_client,
        'video_processor': video_processor
    }

    # Store logger for easy access
    app.logger = logger


def _register_blueprints(app):
    """Register all route blueprints"""
    from .routes import register_blueprints
    register_blueprints(app)


# Convenience functions to get services
def get_service(name):
    """Get a service from current app context"""
    from flask import current_app
    return current_app.extensions.get('services', {}).get(name)


def get_upload_service():
    return get_service('upload')


def get_firebase():
    return get_service('firebase')


def get_uball():
    return get_service('uball')


def get_video_processor():
    return get_service('video_processor')
