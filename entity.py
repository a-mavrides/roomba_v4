
from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN

class RoombaV4Entity(CoordinatorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator, suffix: str) -> None:
        super().__init__(coordinator)
        self.coordinator = coordinator
        robot = coordinator.data.get("robot", {}) if coordinator.data else {}
        self._attr_unique_id = f"{coordinator.robot_blid}_{suffix}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.robot_blid)},
            manufacturer="iRobot",
            model=robot.get("sku") or "Roomba v4",
            name=robot.get("robotName") or robot.get("name") or "Roomba",
            sw_version=robot.get("softwareVer") or robot.get("firmware") or robot.get("robot_version"),
        )
