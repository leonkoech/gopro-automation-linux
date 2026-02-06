"""
Lambda Video Extractor Client

This module provides a client for invoking the video-extractor Lambda function
to extract game clips from S3 video chapters.

When chapters are in S3, the Jetson's FFmpeg cannot read them directly (HTTPS
protocol not in whitelist). This client invokes a Lambda function that:
1. Reads chapters from S3 using presigned URLs
2. Extracts the game portion using FFmpeg
3. Uploads the result to S3

Usage:
    from lambda_extractor import LambdaExtractor

    extractor = LambdaExtractor()
    result = extractor.extract_game_clip(
        chapters=[{"s3_key": "raw-chapters/session/ch1.MP4", "duration_seconds": 1800}],
        bucket="uball-videos-production",
        offset_seconds=300.0,
        duration_seconds=2400.0,
        output_s3_key="games/2026-01-20/game1/FL.mp4",
        compress_to_1080p=True
    )

    if result['success']:
        print(f"Extracted to: {result['output_s3_uri']}")
"""

import json
import os
import time
from typing import Dict, Any, List, Optional

import boto3
from botocore.config import Config

from logging_service import get_logger

logger = get_logger('gopro.lambda_extractor')

# Lambda function name (deployed via SAM template)
LAMBDA_FUNCTION_NAME = os.getenv('VIDEO_EXTRACTOR_LAMBDA', 'uball-video-extractor')

# AWS region
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')


class LambdaExtractor:
    """
    Client for the video-extractor Lambda function.

    Extracts game clips from S3 video chapters using Lambda + FFmpeg.
    """

    def __init__(
        self,
        function_name: str = None,
        region: str = None,
        timeout: int = 900
    ):
        """
        Initialize Lambda extractor client.

        Args:
            function_name: Lambda function name (default: VIDEO_EXTRACTOR_LAMBDA env var)
            region: AWS region (default: AWS_REGION env var)
            timeout: Lambda timeout in seconds (default: 900 = 15 min)
        """
        self.function_name = function_name or LAMBDA_FUNCTION_NAME
        self.region = region or AWS_REGION
        self.timeout = timeout

        # Configure boto3 with extended timeout for Lambda
        config = Config(
            read_timeout=timeout + 60,  # Extra buffer for response
            connect_timeout=30,
            retries={'max_attempts': 1}  # Don't retry video processing
        )

        self.lambda_client = boto3.client(
            'lambda',
            region_name=self.region,
            config=config
        )

        logger.info(f"LambdaExtractor initialized: {self.function_name} in {self.region}")

    def extract_game_clip(
        self,
        chapters: List[Dict[str, Any]],
        bucket: str,
        offset_seconds: float,
        duration_seconds: float,
        output_s3_key: str,
        compress_to_1080p: bool = True,
        add_buffer_seconds: float = 30.0,
        batch_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Extract a game clip from S3 chapters using Lambda.

        Args:
            chapters: List of chapter info dicts with 's3_key' and 'duration_seconds'
            bucket: S3 bucket name
            offset_seconds: Start position in seconds from first chapter
            duration_seconds: Duration of clip to extract
            output_s3_key: S3 key for output file
            compress_to_1080p: If True, compress to 1080p (default True)
            add_buffer_seconds: Extra seconds to add before/after (default 30s)
            batch_config: Optional dict to submit AWS Batch job after extraction
                          Keys: job_queue, job_definition, final_s3_key, game_id, angle

        Returns:
            Dict with success, output_s3_uri, output_size_bytes, processing_time_seconds,
            batch_job (if batch_config provided), error
        """
        # Build Lambda payload
        payload = {
            'chapters': [
                {
                    's3_key': ch.get('s3_key'),
                    'duration_seconds': ch.get('duration_seconds', 0)
                }
                for ch in chapters
            ],
            'bucket': bucket,
            'offset_seconds': offset_seconds,
            'duration_seconds': duration_seconds,
            'output_s3_key': output_s3_key,
            'compress_to_1080p': compress_to_1080p,
            'add_buffer_seconds': add_buffer_seconds
        }

        # Add batch config if provided (Lambda will submit Batch job)
        if batch_config:
            payload['batch_config'] = batch_config
            logger.info(f"  Lambda will submit Batch job after extraction")

        logger.info(f"Invoking Lambda: {self.function_name}")
        logger.info(f"  Chapters: {len(chapters)}")
        logger.info(f"  Offset: {offset_seconds}s, Duration: {duration_seconds}s")
        logger.info(f"  Output: s3://{bucket}/{output_s3_key}")
        logger.info(f"  Compress: {compress_to_1080p}")

        start_time = time.time()

        try:
            # Invoke Lambda synchronously (wait for result)
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',  # Synchronous
                Payload=json.dumps(payload)
            )

            # Check for Lambda execution errors
            if response.get('FunctionError'):
                error_payload = json.loads(response['Payload'].read())
                error_msg = error_payload.get('errorMessage', 'Unknown Lambda error')
                logger.error(f"Lambda execution error: {error_msg}")
                return {
                    'success': False,
                    'output_s3_uri': None,
                    'output_size_bytes': 0,
                    'processing_time_seconds': time.time() - start_time,
                    'error': f"Lambda error: {error_msg}"
                }

            # Parse response
            result = json.loads(response['Payload'].read())

            elapsed = time.time() - start_time
            logger.info(f"Lambda completed in {elapsed:.1f}s")

            if result.get('success'):
                logger.info(f"  Output: {result.get('output_s3_uri')}")
                logger.info(f"  Size: {result.get('output_size_bytes', 0) / (1024*1024):.1f} MB")
            else:
                logger.error(f"  Error: {result.get('error')}")

            return result

        except self.lambda_client.exceptions.ResourceNotFoundException:
            error_msg = f"Lambda function not found: {self.function_name}"
            logger.error(error_msg)
            return {
                'success': False,
                'output_s3_uri': None,
                'output_size_bytes': 0,
                'processing_time_seconds': time.time() - start_time,
                'error': error_msg
            }

        except Exception as e:
            error_msg = f"Lambda invocation failed: {str(e)}"
            logger.error(error_msg)
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'output_s3_uri': None,
                'output_size_bytes': 0,
                'processing_time_seconds': time.time() - start_time,
                'error': error_msg
            }

    def extract_game_clip_async(
        self,
        chapters: List[Dict[str, Any]],
        bucket: str,
        offset_seconds: float,
        duration_seconds: float,
        output_s3_key: str,
        compress_to_1080p: bool = True,
        add_buffer_seconds: float = 30.0
    ) -> Dict[str, Any]:
        """
        Start async extraction - returns immediately, Lambda runs in background.

        Use this for fire-and-forget scenarios where you'll check S3 later.

        Returns:
            Dict with request_id for tracking (check S3 for result)
        """
        payload = {
            'chapters': [
                {
                    's3_key': ch.get('s3_key'),
                    'duration_seconds': ch.get('duration_seconds', 0)
                }
                for ch in chapters
            ],
            'bucket': bucket,
            'offset_seconds': offset_seconds,
            'duration_seconds': duration_seconds,
            'output_s3_key': output_s3_key,
            'compress_to_1080p': compress_to_1080p,
            'add_buffer_seconds': add_buffer_seconds
        }

        logger.info(f"Invoking Lambda async: {self.function_name}")

        try:
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='Event',  # Async - returns immediately
                Payload=json.dumps(payload)
            )

            request_id = response.get('ResponseMetadata', {}).get('RequestId', 'unknown')
            status_code = response.get('StatusCode', 0)

            if status_code == 202:  # Accepted
                logger.info(f"Lambda async invocation accepted: {request_id}")
                return {
                    'success': True,
                    'async': True,
                    'request_id': request_id,
                    'expected_output': f"s3://{bucket}/{output_s3_key}",
                    'error': None
                }
            else:
                return {
                    'success': False,
                    'async': True,
                    'request_id': request_id,
                    'expected_output': None,
                    'error': f"Unexpected status code: {status_code}"
                }

        except Exception as e:
            logger.error(f"Lambda async invocation failed: {e}")
            return {
                'success': False,
                'async': True,
                'request_id': None,
                'expected_output': None,
                'error': str(e)
            }

    def check_output_exists(self, bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        Check if extraction output exists in S3.

        Useful for checking async extraction results.

        Returns:
            Dict with exists, size_bytes, last_modified
        """
        try:
            s3_client = boto3.client('s3')
            response = s3_client.head_object(Bucket=bucket, Key=s3_key)
            return {
                'exists': True,
                'size_bytes': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'error': None
            }
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {
                    'exists': False,
                    'size_bytes': 0,
                    'last_modified': None,
                    'error': None
                }
            return {
                'exists': False,
                'size_bytes': 0,
                'last_modified': None,
                'error': str(e)
            }


# Singleton instance for convenience
_extractor_instance: Optional[LambdaExtractor] = None


def get_lambda_extractor() -> LambdaExtractor:
    """Get or create singleton LambdaExtractor instance."""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = LambdaExtractor()
    return _extractor_instance


# Convenience function for direct use
def extract_with_lambda(
    chapters: List[Dict[str, Any]],
    bucket: str,
    offset_seconds: float,
    duration_seconds: float,
    output_s3_key: str,
    compress_to_1080p: bool = True,
    add_buffer_seconds: float = 30.0
) -> Dict[str, Any]:
    """
    Convenience function to extract using Lambda.

    See LambdaExtractor.extract_game_clip() for details.
    """
    extractor = get_lambda_extractor()
    return extractor.extract_game_clip(
        chapters=chapters,
        bucket=bucket,
        offset_seconds=offset_seconds,
        duration_seconds=duration_seconds,
        output_s3_key=output_s3_key,
        compress_to_1080p=compress_to_1080p,
        add_buffer_seconds=add_buffer_seconds
    )
