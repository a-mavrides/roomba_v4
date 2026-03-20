
from __future__ import annotations

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .cloud_api import IRobotCloudApi
from .const import CONF_AUTO_DOWNLOAD_MAP, CONF_COUNTRY_CODE, CONF_DEBUG_ENABLED, CONF_ROBOT_BLID, CONF_S3_MAP_URL, DOMAIN, PLATFORMS
from .coordinator import RoombaV4Coordinator
from .debug import get_debug_enabled

type RoombaV4ConfigEntry = ConfigEntry[dict]

SERVICE_REFRESH_MAP_FROM_ARCHIVE = "refresh_map_from_archive"
SERVICE_REFRESH_MAP_FROM_URL = "refresh_map_from_url"
SERVICE_CLEAR_PATH_HISTORY = "clear_path_history"
SERVICE_CLEAN_ROOM = "clean_room"

async def _async_reload_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    session = async_get_clientsession(hass)
    api = IRobotCloudApi(
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        country_code=entry.data.get(CONF_COUNTRY_CODE, "US"),
        session=session,
    )
    api.local_mqtt_ip_override = ""
    debug_enabled = get_debug_enabled(entry)
    coordinator = RoombaV4Coordinator(
        hass=hass,
        api=api,
        robot_blid=entry.data[CONF_ROBOT_BLID],
        entry_id=entry.entry_id,
        auto_download_map=entry.options.get(CONF_AUTO_DOWNLOAD_MAP, entry.data.get(CONF_AUTO_DOWNLOAD_MAP, True)),
        s3_map_url=entry.options.get(CONF_S3_MAP_URL, entry.data.get(CONF_S3_MAP_URL)) or None,
        debug_enabled=debug_enabled,
    )
    api.debug_dir = coordinator.debug_dir if debug_enabled else None
    await coordinator.async_config_entry_first_refresh()
    await coordinator.async_start_background_subscriber()
    hass.data[DOMAIN][entry.entry_id] = coordinator
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))

    if not hass.services.has_service(DOMAIN, SERVICE_REFRESH_MAP_FROM_ARCHIVE):
        async def _handle_refresh_archive(call: ServiceCall) -> None:
            archive_path = call.data["archive_path"]
            show_labels = call.data.get("show_labels", True)
            show_coverage = call.data.get("show_coverage", True)
            for stored_coordinator in hass.data[DOMAIN].values():
                await stored_coordinator.async_refresh_map_from_archive(archive_path, show_labels=show_labels, show_coverage=show_coverage)

        hass.services.async_register(
            DOMAIN,
            SERVICE_REFRESH_MAP_FROM_ARCHIVE,
            _handle_refresh_archive,
            schema=vol.Schema({
                vol.Required("archive_path"): str,
                vol.Optional("show_labels", default=True): bool,
                vol.Optional("show_coverage", default=True): bool,
            }),
        )

    if not hass.services.has_service(DOMAIN, SERVICE_REFRESH_MAP_FROM_URL):
        async def _handle_refresh_url(call: ServiceCall) -> None:
            url = call.data["url"]
            for stored_coordinator in hass.data[DOMAIN].values():
                await stored_coordinator.async_refresh_map_from_url(url)

        hass.services.async_register(
            DOMAIN,
            SERVICE_REFRESH_MAP_FROM_URL,
            _handle_refresh_url,
            schema=vol.Schema({vol.Required("url"): str}),
        )


    if not hass.services.has_service(DOMAIN, SERVICE_CLEAN_ROOM):
        async def _handle_clean_room(call: ServiceCall) -> None:
            room_name = call.data["room_name"]
            target_entry_id = call.data.get("entry_id")
            coordinators = hass.data[DOMAIN].values()
            if target_entry_id:
                coordinator = hass.data[DOMAIN].get(target_entry_id)
                if coordinator is None:
                    raise vol.Invalid(f"Unknown roomba_v4 entry_id: {target_entry_id}")
                coordinators = [coordinator]
            handled = False
            for stored_coordinator in coordinators:
                if room_name in (stored_coordinator.rooms or []):
                    await stored_coordinator.async_clean_room_by_name(room_name)
                    handled = True
            if not handled:
                raise vol.Invalid(f"Room not found in available roomba_v4 coordinators: {room_name}")

        hass.services.async_register(
            DOMAIN,
            SERVICE_CLEAN_ROOM,
            _handle_clean_room,
            schema=vol.Schema({
                vol.Required("room_name"): str,
                vol.Optional("entry_id"): str,
            }),
        )

    if not hass.services.has_service(DOMAIN, SERVICE_CLEAR_PATH_HISTORY):
        async def _handle_clear_path_history(call: ServiceCall) -> None:
            for stored_coordinator in hass.data[DOMAIN].values():
                camera = getattr(stored_coordinator, "map_camera", None)
                if camera is not None:
                    await camera.async_clear_path_history()
                stored_coordinator.async_update_listeners()

        hass.services.async_register(
            DOMAIN,
            SERVICE_CLEAR_PATH_HISTORY,
            _handle_clear_path_history,
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True

async def async_unload_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry) -> bool:
    coordinator = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if coordinator is not None:
        await coordinator.async_shutdown()
        await coordinator.api.async_shutdown_event_subscriber()
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
