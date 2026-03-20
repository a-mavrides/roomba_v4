from __future__ import annotations

from homeassistant.components.vacuum import StateVacuumEntity, VacuumActivity, VacuumEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.icon import icon_for_battery_level

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
    async_add_entities([RoombaV4Vacuum(coordinator)])


class RoombaV4Vacuum(RoombaV4Entity, StateVacuumEntity):
    _attr_name = "Vacuum"
    _attr_supported_features = (
        VacuumEntityFeature.START
        | VacuumEntityFeature.PAUSE
        | VacuumEntityFeature.STOP
        | VacuumEntityFeature.RETURN_HOME
        | VacuumEntityFeature.STATE
        | getattr(VacuumEntityFeature, "SEND_COMMAND", 0)
    )

    def __init__(self, coordinator) -> None:
        RoombaV4Entity.__init__(self, coordinator, "vacuum")

    @property
    def activity(self):
        data = self.coordinator.data or {}
        status = data.get("status") or {}
        raw = str(status.get("vacuum_state") or data.get("vacuum_state") or "unknown").lower()
        mapping = {
            "cleaning": VacuumActivity.CLEANING,
            "docked": VacuumActivity.DOCKED,
            "idle": VacuumActivity.IDLE,
            "paused": VacuumActivity.PAUSED,
            "returning": VacuumActivity.RETURNING,
            "error": VacuumActivity.ERROR,
        }
        return mapping.get(raw, VacuumActivity.IDLE)

    @property
    def state(self):
        data = self.coordinator.data or {}
        status = data.get("status") or {}
        return status.get("vacuum_state") or data.get("vacuum_state") or "unknown"

    async def async_start(self) -> None:
        await self.coordinator.async_start_clean_all()

    async def async_pause(self) -> None:
        await self.coordinator.async_pause_cleaning()

    async def async_stop(self, **kwargs) -> None:
        await self.coordinator.async_stop_cleaning()

    async def async_return_to_base(self, **kwargs) -> None:
        await self.coordinator.async_return_to_base()

    async def async_send_command(self, command, params=None, **kwargs) -> None:
        cmd = str(command or "").strip().lower()
        payload = params
        if cmd == "app_segment_clean":
            if payload is None:
                raise ValueError("app_segment_clean requires params")
            if not isinstance(payload, list):
                payload = [payload]
            await self.coordinator.async_clean_rooms_by_selection_ids(payload)
            return
        if cmd in {"start", "clean_all"}:
            await self.coordinator.async_start_clean_all()
            return
        raise ValueError(f"Unsupported send_command: {command}")

    @property
    def extra_state_attributes(self):
        data = self.coordinator.data or {}
        robot = data.get("robot") or {}
        room_pairs = [
            {
                "id": str(item.get("control_room_id") or (item.get("properties") or {}).get("control_room_id") or (item.get("properties") or {}).get("room_id") or item.get("room_id") or item.get("id")),
                "name": str(item.get("name")),
            }
            for item in (self.coordinator.room_info or [])
            if item.get("name")
        ]
        status = data.get("status") or {}
        return {
            "robot_name": robot.get("robotName") or robot.get("name"),
            "sku": robot.get("sku"),
            "firmware": robot.get("softwareVer") or robot.get("firmware") or robot.get("robot_version"),
            "active_map_id": data.get("active_map_id"),
            "active_map_version": data.get("active_map_version"),
            "selected_room": self.coordinator.selected_room,
            "rooms": self.coordinator.rooms,
            "room_pairs": room_pairs,
            "commands_supported_yet": True,
            "clean_selected_room_button_behavior": "select a room and run Clean Selected Room, or use the roomba_v4.clean_room service",
            "room_control_supported": bool(self.coordinator.rooms),
            "room_cleaning_foundation_ready": bool(self.coordinator.rooms),
            "battery": status.get("battery"),
            "cleaning_mode": status.get("cleaning_mode") or self.coordinator.derived_cleaning_mode(),
            "operating_mode": status.get("operating_mode") or self.coordinator.current_operating_mode_value(),
            "operating_mode_label": status.get("operating_mode_label") or self.coordinator.current_operating_mode_label(),
            "preferred_cleaning_mode": self.coordinator.preferred_cleaning_mode(),
            "preferred_suction_level": self.coordinator.preferred_suction_level(),
            "preferred_water_level": self.coordinator.preferred_water_level(),
            "mission_phase": status.get("mission_phase"),
            "mission_cycle": status.get("mission_cycle"),
            "mission_error": status.get("mission_error"),
            "mission_not_ready": status.get("mission_not_ready"),
            "position_x": status.get("x"),
            "position_y": status.get("y"),
            "position_theta": status.get("theta"),
            "dock_known": status.get("dock_known"),
            "dock_contact": status.get("dock_contact"),
            "wifi_rssi": status.get("wifi_rssi"),
            "wifi_snr": status.get("wifi_snr"),
            "bin_present": status.get("bin_present"),
            "bin_full": status.get("bin_full"),
            "last_topic": status.get("last_topic"),
            "last_update": status.get("last_update"),
            "charging": _status_is_charging(status),
            "battery_icon": icon_for_battery_level(
                battery_level=status.get("battery"),
                charging=_status_is_charging(status),
            ),
            "last_event": ((data.get("_event_state") or {}).get("last_event_type")),
            "last_event_title": ((data.get("_event_state") or {}).get("last_event_title")),
            "last_event_message": ((data.get("_event_state") or {}).get("last_event_message")),
            "last_event_time": ((data.get("_event_state") or {}).get("last_event_time")),
        }
