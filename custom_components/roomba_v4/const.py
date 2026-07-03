
from datetime import timedelta

DOMAIN = "roomba_v4"
PLATFORMS = ["camera", "sensor", "vacuum", "button", "select"]

CONF_COUNTRY_CODE = "country_code"
CONF_ROBOT_BLID = "robot_blid"
CONF_AUTO_DOWNLOAD_MAP = "auto_download_map"
CONF_S3_MAP_URL = "s3_map_url"
CONF_DEBUG_ENABLED = "debug_enabled"

DEFAULT_COUNTRY_CODE = "GB"
UPDATE_INTERVAL = timedelta(minutes=10)
ACTIVE_UPDATE_INTERVAL = timedelta(seconds=20)
IDLE_UPDATE_INTERVAL = timedelta(minutes=2)
DOCKED_UPDATE_INTERVAL = timedelta(minutes=10)

EVENT_TYPE = "roomba_v4_event"
LIVE_STATUS_DEBUG_DIR = ".storage/roomba_v4_debug/{entry_id}"
LEGACY_DEBUG_DIR = ".storage/roomba_v4_debug"

STORAGE_VERSION = 1
STORAGE_KEY_PREFIX = "roomba_v4"

API_USER_AGENT = "iRobot/7.16.2.140449 CFNetwork/1568.100.1.2.1 Darwin/24.0.0"
AWS_USER_AGENT = "aws-sdk-iOS/2.27.6 iOS/18.0.1 en_US"
