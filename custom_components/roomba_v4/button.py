from __future__ import annotations

from pathlib import Path

from homeassistant.components.button import ButtonEntity
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from .cloud_api import CloudApiError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import RoombaV4ConfigEntry
from .const import DOMAIN
from .entity import RoombaV4Entity


async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        RefreshMapButton(coordinator),
        DownloadMapFromUrlButton(coordinator),
        DeleteCachedMapsAndFetchLatestButton(coordinator),
        CleanAllRoomsButton(coordinator),
        CleanSelectedRoomButton(coordinator),
        SpotCleanButton(coordinator),
        PauseCleaningButton(coordinator),
        ResumeCleaningButton(coordinator),
        StopCleaningButton(coordinator),
        ReturnToBaseButton(coordinator),
        ClearPathTrailButton(coordinator),
    ])


class RefreshMapButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Refresh Map From Archive"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "refresh_map")

    async def async_press(self) -> None:
        archive_path = Path(self.coordinator.map_archive_path)
        if archive_path.exists():
            await self.coordinator.async_refresh_map_from_archive(archive_path)
        else:
            self.coordinator.async_update_listeners()


class DownloadMapFromUrlButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Download Map From URL"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "download_map")

    async def async_press(self) -> None:
        if self.coordinator.s3_map_url:
            await self.coordinator.async_refresh_map_from_url(self.coordinator.s3_map_url)
        else:
            self.coordinator.async_update_listeners()


class CleanSelectedRoomButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Clean Selected Room"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "clean_selected_room")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_clean_selected_room()
        except CloudApiError as err:
            raise HomeAssistantError(f"Clean Selected Room failed: {err}") from err



class DeleteCachedMapsAndFetchLatestButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Delete Cached Maps & Fetch Latest"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "delete_cached_maps_fetch_latest")

    async def async_press(self) -> None:
        await self.coordinator.async_delete_cached_maps_and_fetch_latest()


class CleanAllRoomsButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Clean All Rooms"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "clean_all_rooms")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_start_clean_all()
        except CloudApiError as err:
            raise HomeAssistantError(f"Clean All Rooms failed: {err}") from err


class SpotCleanButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Spot Clean"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "spot_clean")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_execute_named_routine("spot_clean")
        except CloudApiError as err:
            raise HomeAssistantError(f"Spot Clean failed: {err}") from err


class PauseCleaningButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Pause Cleaning"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "pause_cleaning")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_pause_cleaning()
        except CloudApiError as err:
            raise HomeAssistantError(f"Pause Cleaning failed: {err}") from err


class ResumeCleaningButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Resume Cleaning"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "resume_cleaning")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_resume_cleaning()
        except CloudApiError as err:
            raise HomeAssistantError(f"Resume Cleaning failed: {err}") from err


class StopCleaningButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Stop Cleaning"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "stop_cleaning")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_stop_cleaning()
        except CloudApiError as err:
            raise HomeAssistantError(f"Stop Cleaning failed: {err}") from err


class ReturnToBaseButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Return To Base"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "return_to_base")

    async def async_press(self) -> None:
        try:
            await self.coordinator.async_return_to_base()
        except CloudApiError as err:
            raise HomeAssistantError(f"Return To Base failed: {err}") from err


class ClearPathTrailButton(RoombaV4Entity, ButtonEntity):
    _attr_name = "Clear Path Trail"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "clear_path_trail")

    async def async_press(self) -> None:
        camera = getattr(self.coordinator, "map_camera", None)
        if camera is not None:
            await camera.async_clear_path_history()
        self.coordinator.async_update_listeners()
