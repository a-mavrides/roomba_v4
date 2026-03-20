from __future__ import annotations

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.icon import icon_for_battery_level
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import RoombaV4ConfigEntry
from .const import DOMAIN
from .entity import RoombaV4Entity


def _status_is_charging(status: dict | None) -> bool:
    status = status or {}
    vacuum_state = str(status.get("vacuum_state") or "").lower()
    mission_phase = str(status.get("mission_phase") or "").lower()
    return bool(
        status.get("dock_contact") is True
        or vacuum_state in {"charging"}
        or mission_phase in {"charge", "chargecompleted", "recharge"}
    )

async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        FirmwareSensor(coordinator),
        SkuSensor(coordinator),
        ActiveMapIdSensor(coordinator),
        ActiveMapVersionSensor(coordinator),
        MapUrlKnownSensor(coordinator),
        RoomsCountSensor(coordinator),
        RoutinesCountSensor(coordinator),
        AvailableRoutinesSensor(coordinator),
        StatusMessageSensor(coordinator),
        BatterySensor(coordinator),
        MissionPhaseSensor(coordinator),
        MissionCycleSensor(coordinator),
        MissionErrorSensor(coordinator),
        LastEventSensor(coordinator),
        CleaningModeSensor(coordinator),
        OperatingModeSensor(coordinator),
        PositionXSensor(coordinator),
        PositionYSensor(coordinator),
        PositionThetaSensor(coordinator),
    ])

class _BaseSensor(RoombaV4Entity, SensorEntity):
    def __init__(self, coordinator, suffix: str) -> None:
        RoombaV4Entity.__init__(self, coordinator, suffix)

class FirmwareSensor(_BaseSensor):
    _attr_name = "Firmware"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "firmware")
    @property
    def native_value(self):
        robot = (self.coordinator.data or {}).get("robot") or {}
        return robot.get("softwareVer") or robot.get("firmware") or robot.get("robot_version")

class SkuSensor(_BaseSensor):
    _attr_name = "SKU"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "sku")
    @property
    def native_value(self):
        robot = (self.coordinator.data or {}).get("robot") or {}
        return robot.get("sku")

class ActiveMapIdSensor(_BaseSensor):
    _attr_name = "Active Map ID"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "active_map_id")
    @property
    def native_value(self):
        return (self.coordinator.data or {}).get("active_map_id")

class ActiveMapVersionSensor(_BaseSensor):
    _attr_name = "Active Map Version"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "active_map_version")
    @property
    def native_value(self):
        return (self.coordinator.data or {}).get("active_map_version")

class MapUrlKnownSensor(_BaseSensor):
    _attr_name = "Map Download Available"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "map_url_known")
    @property
    def native_value(self):
        return bool((self.coordinator.data or {}).get("s3_map_url_known"))

class RoomsCountSensor(_BaseSensor):
    _attr_name = "Rooms Count"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "rooms_count")
    @property
    def native_value(self):
        return len(self.coordinator.rooms)

class RoutinesCountSensor(_BaseSensor):
    _attr_name = "Routines Count"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "routines_count")
    @property
    def native_value(self):
        routines = (self.coordinator.data or {}).get("routines") or []
        return len(routines) if isinstance(routines, list) else 0

class AvailableRoutinesSensor(_BaseSensor):
    _attr_name = "Available Routines"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "available_routines")
    @property
    def native_value(self):
        routines = (self.coordinator.data or {}).get("routines") or []
        names = []
        if isinstance(routines, list):
            for item in routines:
                if not isinstance(item, dict):
                    continue
                name = item.get("name") or item.get("routine_type") or item.get("command")
                if name:
                    names.append(str(name))
        return ", ".join(names) if names else None


class _StatusSensor(_BaseSensor):
    def _status(self):
        return (self.coordinator.data or {}).get("status") or {}


class StatusMessageSensor(_StatusSensor):
    _attr_name = "Status Message"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "status_message")

    @property
    def native_value(self):
        return self._status().get("status_message")

    @property
    def extra_state_attributes(self):
        status = self._status()
        live_state = (self.coordinator.data or {}).get("live_state") or {}
        livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
        return {
            "tank_present": status.get("tank_present"),
            "detected_pad": status.get("detected_pad"),
            "mission_phase": status.get("mission_phase"),
            "mission_cycle": status.get("mission_cycle"),
            "mission_error": status.get("mission_error"),
            "operating_mode": status.get("operating_mode"),
            "operating_mode_label": status.get("operating_mode_label"),
            "cleaning_mode": status.get("cleaning_mode"),
            "mission_not_ready": status.get("mission_not_ready"),
            "bin_present": status.get("bin_present"),
            "bin_full": status.get("bin_full"),
            "livemap_path_points_count": livemap.get("path_points_count"),
            "livemap_path_kind": livemap.get("path_kind"),
            "livemap_path_timestamp": livemap.get("path_timestamp"),
        }


class BatterySensor(_StatusSensor):
    _attr_name = "Battery"
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_native_unit_of_measurement = "%"
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "battery")

    @property
    def native_value(self):
        return self._status().get("battery")

    @property
    def icon(self):
        return icon_for_battery_level(
            battery_level=self.native_value,
            charging=_status_is_charging(self._status()),
        )

    @property
    def extra_state_attributes(self):
        return {
            "charging": _status_is_charging(self._status()),
            "battery_icon": self.icon,
        }


class MissionPhaseSensor(_StatusSensor):
    _attr_name = "Mission Phase"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "mission_phase")
    @property
    def native_value(self):
        return self._status().get("mission_phase")


class MissionCycleSensor(_StatusSensor):
    _attr_name = "Mission Cycle"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "mission_cycle")
    @property
    def native_value(self):
        return self._status().get("mission_cycle")


class MissionErrorSensor(_StatusSensor):
    _attr_name = "Mission Error"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "mission_error")
    @property
    def native_value(self):
        return self._status().get("mission_error")






class CleaningModeSensor(_StatusSensor):
    _attr_name = "Cleaning Mode"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "cleaning_mode")
    @property
    def native_value(self):
        return self._status().get("cleaning_mode") or self.coordinator.derived_cleaning_mode()
    @property
    def extra_state_attributes(self):
        return {
            "preferred_mode": self.coordinator.preferred_cleaning_mode(),
            "supported_modes": self.coordinator.cleaning_mode_options(),
            "tank_present": self._status().get("tank_present"),
            "detected_pad": self._status().get("detected_pad"),
        }


class OperatingModeSensor(_StatusSensor):
    _attr_name = "Operating Mode"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "operating_mode")
    @property
    def native_value(self):
        value = self._status().get("operating_mode")
        return str(value) if value is not None else None
    @property
    def extra_state_attributes(self):
        return {
            "label": self._status().get("operating_mode_label") or self.coordinator.current_operating_mode_label(),
            "supported_operating_modes_bitmask": ((self.coordinator.data or {}).get("robot") or {}).get("cap", {}).get("oMode"),
            "supported_suction_levels": self.coordinator.suction_level_options(),
            "supported_water_levels": self.coordinator.water_level_options(),
        }

class LastEventSensor(_BaseSensor):
    _attr_name = "Last Event"

    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "last_event")

    @property
    def native_value(self):
        event_state = ((self.coordinator.data or {}).get("_event_state") or {})
        return event_state.get("last_event_type")

    @property
    def extra_state_attributes(self):
        event_state = ((self.coordinator.data or {}).get("_event_state") or {})
        return {
            "title": event_state.get("last_event_title"),
            "message": event_state.get("last_event_message"),
            "event_time": event_state.get("last_event_time"),
        }

class _PositionSensor(_StatusSensor):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_entity_registry_enabled_default = False


class PositionXSensor(_PositionSensor):
    _attr_name = "Position X"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "position_x")
    @property
    def native_value(self):
        return self._status().get("x")


class PositionYSensor(_PositionSensor):
    _attr_name = "Position Y"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "position_y")
    @property
    def native_value(self):
        return self._status().get("y")


class PositionThetaSensor(_PositionSensor):
    _attr_name = "Position Theta"
    def __init__(self, coordinator) -> None:
        super().__init__(coordinator, "position_theta")
    @property
    def native_value(self):
        return self._status().get("theta")
