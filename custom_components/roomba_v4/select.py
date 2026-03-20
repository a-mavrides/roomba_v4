from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import RoombaV4ConfigEntry
from .const import DOMAIN
from .entity import RoombaV4Entity


async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SelectEntity] = [RoombaRoomSelect(coordinator), PreferredCleaningModeSelect(coordinator)]
    if coordinator.suction_level_options():
        entities.append(PreferredSuctionLevelSelect(coordinator))
    if coordinator.water_level_options():
        entities.append(PreferredWaterLevelSelect(coordinator))
    async_add_entities(entities)


class RoombaRoomSelect(RoombaV4Entity, SelectEntity):
    _attr_name = "Room"
    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "room_select")

    @property
    def options(self) -> list[str]:
        return self.coordinator.rooms or []

    @property
    def current_option(self) -> str | None:
        return self.coordinator.selected_room

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_select_room(option)


class _BasePreferenceSelect(RoombaV4Entity, SelectEntity):
    _attr_entity_category = None

    @property
    def extra_state_attributes(self):
        return {
            "note": "This select stores the preference Home Assistant will send with the next supported cleaning command.",
        }


class PreferredCleaningModeSelect(_BasePreferenceSelect):
    _attr_name = "Preferred Cleaning Mode"
    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "preferred_cleaning_mode")

    @property
    def options(self) -> list[str]:
        return self.coordinator.cleaning_mode_options()

    @property
    def current_option(self) -> str | None:
        return self.coordinator.preferred_cleaning_mode()

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_preferred_cleaning_mode(option)


class PreferredSuctionLevelSelect(_BasePreferenceSelect):
    _attr_name = "Preferred Suction Level"
    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "preferred_suction_level")

    @property
    def options(self) -> list[str]:
        return self.coordinator.suction_level_options()

    @property
    def current_option(self) -> str | None:
        return self.coordinator.preferred_suction_level()

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_preferred_suction_level(option)


class PreferredWaterLevelSelect(_BasePreferenceSelect):
    _attr_name = "Preferred Water Level"
    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "preferred_water_level")

    @property
    def options(self) -> list[str]:
        return self.coordinator.water_level_options()

    @property
    def current_option(self) -> str | None:
        return self.coordinator.preferred_water_level()

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_preferred_water_level(option)
