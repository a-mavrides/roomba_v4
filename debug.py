from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .const import CONF_DEBUG_ENABLED, DOMAIN, LIVE_STATUS_DEBUG_DIR, LEGACY_DEBUG_DIR

_LOGGER = logging.getLogger(__name__)


def get_debug_enabled(entry: Any) -> bool:
    options = getattr(entry, "options", {}) or {}
    data = getattr(entry, "data", {}) or {}
    return bool(options.get(CONF_DEBUG_ENABLED, data.get(CONF_DEBUG_ENABLED, False)))


def get_debug_dir(hass, entry_id: str) -> Path:
    return Path(hass.config.path(LIVE_STATUS_DEBUG_DIR.format(entry_id=entry_id)))


def get_legacy_debug_dir(hass) -> Path:
    return Path(hass.config.path(LEGACY_DEBUG_DIR))


def is_debug_enabled_for_entry_id(hass, entry_id: str) -> bool:
    coordinator = hass.data.get(DOMAIN, {}).get(entry_id)
    if coordinator is not None:
        return bool(getattr(coordinator, "debug_enabled", False))

    for config_entry in hass.config_entries.async_entries(DOMAIN):
        if config_entry.entry_id == entry_id:
            return get_debug_enabled(config_entry)
    return False


def debug_log(hass, entry_id: str, logger: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    if is_debug_enabled_for_entry_id(hass, entry_id):
        logger.debug(message, *args, **kwargs)


def ensure_debug_dir(path: Path | None) -> None:
    if path is None:
        return
    path.mkdir(parents=True, exist_ok=True)
