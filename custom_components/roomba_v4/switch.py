from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import RoombaV4ConfigEntry
from .const import DOMAIN
from .entity import RoombaV4Entity


async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([ActiveStateRefreshSwitch(coordinator)])


class ActiveStateRefreshSwitch(RoombaV4Entity, SwitchEntity):
    _attr_name = "Active State Refresh"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_icon = "mdi:refresh-auto"

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "active_state_refresh")

    @property
    def extra_state_attributes(self):
        return {
            "note": "When on, Home Assistant actively pulls the robot's live state each poll (like opening the app), so a docked robot doesn't show stale status. Turn off to only receive state the robot pushes on its own.",
        }

    @property
    def is_on(self) -> bool:
        return bool(self.coordinator.active_state_refresh)

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_set_active_state_refresh(True)

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_set_active_state_refresh(False)
