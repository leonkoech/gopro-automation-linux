"""
AWS Kinesis Video Streams (KVS) GStreamer sink configuration.

Provides helpers for configuring kvssink element properties,
ensuring KVS streams exist, and generating HLS playback URLs.

The KVS GStreamer plugin (amazon-kinesis-video-streams-producer-sdk-cpp)
must be compiled for ARM on each device — see install_deps.sh.
"""

import logging
import boto3
from botocore.exceptions import ClientError

from ..config import ZCamConfig, ZCamCamera

logger = logging.getLogger('zcam.kvs')


def get_kvs_sink_string(camera: ZCamCamera, config: ZCamConfig) -> str:
    """Return the kvssink GStreamer element string for a pipeline.

    AWS credentials are read from environment variables by the KVS plugin:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
    """
    return (
        f'kvssink stream-name={camera.kvs_stream_name} '
        f'aws-region={config.kvs_region} '
        f'streaming-type=0 '  # 0 = REALTIME
        f'fragment-duration={config.kvs_fragment_duration}'
    )


def ensure_kvs_streams(config: ZCamConfig):
    """Create KVS streams if they don't already exist."""
    client = boto3.client('kinesisvideo', region_name=config.kvs_region)

    for cam in config.cameras:
        try:
            client.describe_stream(StreamName=cam.kvs_stream_name)
            logger.info(f"KVS stream exists: {cam.kvs_stream_name}")
        except client.exceptions.ResourceNotFoundException:
            logger.info(f"Creating KVS stream: {cam.kvs_stream_name}")
            client.create_stream(
                StreamName=cam.kvs_stream_name,
                DataRetentionInHours=24,
                MediaType='video/h264',
            )
            logger.info(f"Created KVS stream: {cam.kvs_stream_name}")
        except ClientError as e:
            logger.error(f"Error checking KVS stream {cam.kvs_stream_name}: {e}")


def verify_kvs_credentials(config: ZCamConfig) -> bool:
    """Verify AWS credentials can access KVS."""
    try:
        client = boto3.client('kinesisvideo', region_name=config.kvs_region)
        client.list_streams(MaxResults=1)
        return True
    except ClientError as e:
        logger.error(f"KVS credential check failed: {e}")
        return False


def get_kvs_playback_url(stream_name: str, config: ZCamConfig) -> str:
    """Get HLS playback URL for a KVS stream (for frontend live preview).

    Returns a signed HLS URL valid for 1 hour.
    """
    client = boto3.client('kinesisvideo', region_name=config.kvs_region)

    # Get the data endpoint for HLS
    endpoint_response = client.get_data_endpoint(
        StreamName=stream_name,
        APIName='GET_HLS_STREAMING_SESSION_URL'
    )
    endpoint = endpoint_response['DataEndpoint']

    # Get the HLS URL from the archived media client
    hls_client = boto3.client(
        'kinesis-video-archived-media',
        endpoint_url=endpoint,
        region_name=config.kvs_region,
    )

    url_response = hls_client.get_hls_streaming_session_url(
        StreamName=stream_name,
        PlaybackMode='LIVE',
        HLSFragmentSelector={
            'FragmentSelectorType': 'SERVER_TIMESTAMP',
        },
        ContainerFormat='FRAGMENTED_MP4',
        DisplayFragmentTimestamp='NEVER',
        Expires=3600,
    )
    return url_response['HLSStreamingSessionURL']
