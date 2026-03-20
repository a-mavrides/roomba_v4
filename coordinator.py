from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.storage import Store
from homeassistant.helpers.service import async_call_from_config
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .cloud_api import CloudApiError, IRobotCloudApi
from .const import ACTIVE_UPDATE_INTERVAL, DOCKED_UPDATE_INTERVAL, EVENT_TYPE, IDLE_UPDATE_INTERVAL, STORAGE_KEY_PREFIX, STORAGE_VERSION, UPDATE_INTERVAL
from .debug import get_debug_dir, get_legacy_debug_dir
from .map_renderer import extract_map_render_metadata, extract_room_info_from_archive, room_names_from_info, render_archive_to_png_bytes

_LOGGER = logging.getLogger(__name__)


MAP_ID_KEYS = (
    "pmap_id",
    "map_id",
    "active_map_id",
    "id",
    "p2map_id",
)
MAP_VERSION_KEYS = (
    "active_p2mapv_id",
    "active_pmapv_id",
    "pmapv_id",
    "version_id",
    "active_mapv_id",
    "map_version",
    "p2mapv_id",
)
URL_HINT_TOKENS = (
    "p2mapv_geojson.tgz",
    "geojson.tgz",
    "amazonaws",
    "livemap",
    "presign",
    "signed",
    "mapv",
    "geojson",
)
DIRECT_URL_KEYS = (
    "livemap_url",
    "live_map_url",
    "p2mapv_geojson_url",
    "geojson_url",
    "download_url",
    "url",
    "href",
    "uri",
)


class RoombaV4Coordinator(DataUpdateCoordinator[dict]):
    def __init__(
        self,
        hass: HomeAssistant,
        api: IRobotCloudApi,
        robot_blid: str,
        entry_id: str,
        auto_download_map: bool = False,
        s3_map_url: str | None = None,
        debug_enabled: bool = False,
    ) -> None:
        super().__init__(hass, _LOGGER, name=f"roomba_v4_{robot_blid}", update_interval=UPDATE_INTERVAL)
        self.api = api
        self.robot_blid = robot_blid
        self.entry_id = entry_id
        self.auto_download_map = auto_download_map
        self.s3_map_url = s3_map_url
        self.debug_enabled = debug_enabled
        self.map_archive_path = self.hass.config.path(f".storage/roomba_v4_{entry_id}_livemap.tgz")
        self.debug_dir = get_debug_dir(self.hass, entry_id)
        self.legacy_debug_dir = get_legacy_debug_dir(self.hass)
        self.map_png_bytes: bytes | None = None
        self.last_map_refresh: str | None = None
        self.room_info: list[dict[str, Any]] = []
        self.rooms: list[str] = []
        self.map_render_metadata: dict[str, Any] = {}
        self.selected_room: str | None = None
        self.store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY_PREFIX}.{entry_id}")
        self._restored = False
        self._restored_data: dict[str, Any] = {}
        self._subscriber_bootstrap_task = None
        self._subscriber_boot_task = None
        self._livemap_trigger_task = None
        self._last_livemap_trigger_at: datetime | None = None
        self._last_livemap_message_at: datetime | None = None
        self._last_good_pose: dict[str, Any] | None = None
        self._last_path_pose: dict[str, Any] | None = None
        self.api.add_live_state_listener(self._handle_live_state_update)
        self._last_event_signature: str | None = None
        self._last_event_at: datetime | None = None


    def _robot_capabilities(self) -> dict[str, Any]:
        robot = (self.data or self._restored_data or {}).get("robot")
        if not isinstance(robot, dict):
            return {}
        cap = robot.get("cap")
        return cap if isinstance(cap, dict) else {}

    def supported_suction_level_count(self) -> int:
        try:
            return max(0, int(self._robot_capabilities().get("suctionLvl") or 0))
        except (TypeError, ValueError):
            return 0

    def supported_water_level_count(self) -> int:
        try:
            return max(0, int(self._robot_capabilities().get("ppWetLvl") or 0))
        except (TypeError, ValueError):
            return 0

    def supports_mopping(self) -> bool:
        try:
            return int(self._robot_capabilities().get("scrub") or 0) > 0
        except (TypeError, ValueError):
            return False

    def suction_level_options(self) -> list[str]:
        count = self.supported_suction_level_count()
        return [f"Level {idx}" for idx in range(1, count + 1)] if count > 0 else []

    def water_level_options(self) -> list[str]:
        count = self.supported_water_level_count()
        return [f"Level {idx}" for idx in range(1, count + 1)] if count > 0 else []

    def cleaning_mode_options(self) -> list[str]:
        return ["Vacuum", "Mop", "Vacuum + Mop"] if self.supports_mopping() else ["Vacuum"]

    def preferred_cleaning_mode(self) -> str:
        preferred = str((self.data or self._restored_data or {}).get("preferred_cleaning_mode") or "").strip()
        return preferred if preferred in self.cleaning_mode_options() else self.derived_cleaning_mode()

    def preferred_suction_level(self) -> str | None:
        preferred = str((self.data or self._restored_data or {}).get("preferred_suction_level") or "").strip()
        return preferred if preferred in self.suction_level_options() else (self.suction_level_options()[0] if self.suction_level_options() else None)

    def preferred_water_level(self) -> str | None:
        preferred = str((self.data or self._restored_data or {}).get("preferred_water_level") or "").strip()
        return preferred if preferred in self.water_level_options() else (self.water_level_options()[0] if self.water_level_options() else None)

    async def async_set_preferred_cleaning_mode(self, option: str) -> None:
        if option not in self.cleaning_mode_options():
            raise CloudApiError(f"Unsupported cleaning mode: {option}")
        base = dict(self.data or self._restored_data or {})
        base["preferred_cleaning_mode"] = option
        self._restored_data = dict(base)
        if isinstance(self.data, dict):
            self.data["preferred_cleaning_mode"] = option
        await self.store.async_save(base)
        self.async_update_listeners()

    async def async_set_preferred_suction_level(self, option: str) -> None:
        if option not in self.suction_level_options():
            raise CloudApiError(f"Unsupported suction level: {option}")
        base = dict(self.data or self._restored_data or {})
        base["preferred_suction_level"] = option
        self._restored_data = dict(base)
        if isinstance(self.data, dict):
            self.data["preferred_suction_level"] = option
        await self.store.async_save(base)
        self.async_update_listeners()

    async def async_set_preferred_water_level(self, option: str) -> None:
        if option not in self.water_level_options():
            raise CloudApiError(f"Unsupported water level: {option}")
        base = dict(self.data or self._restored_data or {})
        base["preferred_water_level"] = option
        self._restored_data = dict(base)
        if isinstance(self.data, dict):
            self.data["preferred_water_level"] = option
        await self.store.async_save(base)
        self.async_update_listeners()

    def current_operating_mode_value(self) -> int | None:
        live_state = (self.data or self._restored_data or {}).get("live_state") or {}
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        value = cms.get("operatingMode") or live_state.get("operatingMode")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def current_operating_mode_label(self) -> str | None:
        value = self.current_operating_mode_value()
        mapping = {1: "mop", 2: "vacuum", 3: "vacuum_and_mop", 6: "vacuum_and_mop"}
        return mapping.get(value, f"unknown_{value}" if value is not None else None)

    def derived_cleaning_mode(self) -> str:
        status = (self.data or self._restored_data or {}).get("status") or {}
        tank_present = bool(status.get("tank_present"))
        detected_pad = str(status.get("detected_pad") or "").strip().lower()
        operating_label = str(self.current_operating_mode_label() or "").lower()

        pad_present = detected_pad not in {"", "nopad", "no_pad", "none", "unknown", "false", "0"}

        if operating_label == "mop":
            return "Mop"
        if operating_label == "vacuum_and_mop":
            return "Vacuum + Mop"
        if operating_label == "vacuum":
            return "Vacuum"
        if tank_present and pad_present:
            return "Vacuum + Mop"
        return "Vacuum"

    def _robot_display_name(self) -> str:
        robot = (self.data or self._restored_data or {}).get("robot") if isinstance((self.data or self._restored_data or {}).get("robot"), dict) else {}
        return str(robot.get("robotName") or robot.get("name") or "Roomba")

    def _vacuum_entity_id(self) -> str | None:
        try:
            ent_reg = er.async_get(self.hass)
            return ent_reg.async_get_entity_id("vacuum", "roomba_v4", f"{self.robot_blid}_vacuum")
        except Exception:
            return None

    def _desired_update_interval(self, vacuum_state: str | None = None) -> timedelta:
        state = str(vacuum_state or ((self.data or self._restored_data or {}).get("vacuum_state") or "")).lower()
        if state in {"cleaning", "returning", "paused", "error"}:
            return ACTIVE_UPDATE_INTERVAL
        if state in {"docked", "charging"}:
            return DOCKED_UPDATE_INTERVAL
        return IDLE_UPDATE_INTERVAL

    def _apply_update_interval(self, vacuum_state: str | None = None) -> None:
        desired = self._desired_update_interval(vacuum_state)
        if self.update_interval != desired:
            self.update_interval = desired
            _LOGGER.debug("roomba_v4 polling interval updated to %s for state=%s", desired, vacuum_state or ((self.data or self._restored_data or {}).get("vacuum_state")))

    async def _emit_significant_event(self, event_type: str, title: str, message: str, *, extra_data: dict[str, Any] | None = None, notification_id: str | None = None) -> None:
        now = datetime.now(tz=UTC)
        signature = f"{event_type}:{message}"
        if self._last_event_signature == signature and self._last_event_at and (now - self._last_event_at) < timedelta(seconds=20):
            return

        payload = {
            "event_type": event_type,
            "robot_id": self.robot_blid,
            "robot_name": self._robot_display_name(),
            "entity_id": self._vacuum_entity_id(),
            "title": title,
            "message": message,
            "ts": now.isoformat(),
        }
        if extra_data:
            payload.update(extra_data)

        base = self.data or self._restored_data or {}
        event_state = dict(base.get("_event_state") or {})
        event_state["last_event_type"] = event_type
        event_state["last_event_title"] = title
        event_state["last_event_message"] = message
        event_state["last_event_time"] = now.isoformat()
        base["_event_state"] = event_state
        self._restored_data = dict(base)
        if isinstance(self.data, dict):
            self.data["_event_state"] = event_state

        self.hass.bus.async_fire(EVENT_TYPE, payload)

        try:
            await self.hass.services.async_call(
                "persistent_notification",
                "create",
                {
                    "title": title,
                    "message": message,
                    "notification_id": notification_id or f"roomba_v4_{self.robot_blid}_{event_type}",
                },
                blocking=True,
            )
        except Exception as err:
            _LOGGER.debug("roomba_v4 notification create failed: %s", err, exc_info=True)

        self._last_event_signature = signature
        self._last_event_at = now

    async def _maybe_emit_status_events(self, previous_status: dict[str, Any], new_status: dict[str, Any], live_state: dict[str, Any]) -> None:
        prev_state = str(previous_status.get("vacuum_state") or "").lower()
        new_state = str(new_status.get("vacuum_state") or "").lower()
        prev_phase = str(previous_status.get("mission_phase") or "").lower()
        new_phase = str(new_status.get("mission_phase") or "").lower()
        prev_cycle = str(previous_status.get("mission_cycle") or "").lower()
        new_cycle = str(new_status.get("mission_cycle") or "").lower()

        prev_error = previous_status.get("mission_error")
        new_error = new_status.get("mission_error")

        def _is_nonzero_error(value: Any) -> bool:
            return value not in (None, 0, "0", "", False)

        base = self.data or self._restored_data or {}
        event_state = dict(base.get("_event_state") or {})
        mission_active = bool(event_state.get("mission_active"))

        if new_state == "cleaning" and prev_state != "cleaning":
            mission_active = True
            await self._emit_significant_event(
                "cleaning_started",
                f"{self._robot_display_name()} started cleaning",
                f"{self._robot_display_name()} has started a cleaning job.",
                extra_data={
                    "battery": new_status.get("battery"),
                    "mission_phase": new_phase,
                    "mission_cycle": new_cycle,
                },
                notification_id=None,
            )

        if _is_nonzero_error(new_error) and (not _is_nonzero_error(prev_error) or new_error != prev_error):
            mission_active = False
            await self._emit_significant_event(
                "mission_error",
                f"{self._robot_display_name()} needs attention",
                f"{self._robot_display_name()} reported mission error {new_error}.",
                extra_data={
                    "battery": new_status.get("battery"),
                    "mission_error": new_error,
                    "mission_phase": new_phase,
                    "mission_cycle": new_cycle,
                },
                notification_id=f"roomba_v4_{self.robot_blid}_mission_error",
            )

        mission_completed = False
        if mission_active and not _is_nonzero_error(new_error):
            became_idle = new_state in {"idle", "docked"} and prev_state in {"cleaning", "paused", "returning"}
            ended_cycle = new_cycle in {"none", "idle", ""} and new_phase in {"stop", "idle", "ready", "charge", "chargecompleted", "dock", "dockend", "recharge", "hmusrdock", "startup_shadow_refresh", ""}
            if became_idle or ended_cycle:
                mission_completed = True

        if mission_completed:
            mission_active = False
            await self._emit_significant_event(
                "cleaning_finished",
                f"{self._robot_display_name()} finished cleaning",
                f"{self._robot_display_name()} finished the cleaning job.",
                extra_data={
                    "battery": new_status.get("battery"),
                    "mission_phase": new_phase,
                    "mission_cycle": new_cycle,
                },
                notification_id=f"roomba_v4_{self.robot_blid}_cleaning_finished",
            )

        if new_state == "docked" and prev_state != "docked":
            await self._emit_significant_event(
                "docked",
                f"{self._robot_display_name()} docked",
                f"{self._robot_display_name()} is back on the dock.",
                extra_data={
                    "battery": new_status.get("battery"),
                    "mission_phase": new_phase,
                    "mission_cycle": new_cycle,
                    "dock_contact": new_status.get("dock_contact"),
                },
                notification_id=f"roomba_v4_{self.robot_blid}_docked",
            )

        event_state["mission_active"] = mission_active
        event_state["last_state"] = new_state
        event_state["last_error"] = new_error
        live_state_meta = live_state.get("_meta") if isinstance(live_state.get("_meta"), dict) else {}
        event_state["last_update"] = live_state_meta.get("last_update") or datetime.now(tz=UTC).isoformat()
        base["_event_state"] = event_state
        self._restored_data = dict(base)

    async def async_start_background_subscriber(self) -> None:
        if self._subscriber_bootstrap_task and not self._subscriber_bootstrap_task.done():
            return

        async def _runner() -> None:
            try:
                self.api.debug_dir = self.debug_dir if self.debug_enabled else None
                if self.debug_enabled:
                    self.legacy_debug_dir.mkdir(parents=True, exist_ok=True)
                if not self.api.robots:
                    await self.api.authenticate()
                await self.api.async_ensure_event_subscriber(self.robot_blid)
            except Exception as err:
                _LOGGER.debug("roomba_v4 always-on subscriber bootstrap failed: %s", err, exc_info=True)
                try:
                    await self.api._write_runtime_debug("mqtt_bootstrap_error", {
                        "robot_id": self.robot_blid,
                        "error": str(err),
                    })
                except Exception:
                    pass

        self._subscriber_bootstrap_task = self.hass.async_create_task(_runner())

    def _deep_merge(self, base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def _is_definitely_docked(self, live_state: dict[str, Any]) -> bool:
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        phase = str(cms.get("phase") or "").lower()
        cycle = str(cms.get("cycle") or "").lower()
        dock = live_state.get("dock") if isinstance(live_state.get("dock"), dict) else {}
        if phase in {"charge", "chargecompleted", "recharge", "hmusrdock", "dockend", "dock"}:
            return True
        if dock.get("contact") is True:
            return True
        if dock.get("known") is True and cycle in {"none", "idle", ""}:
            return True
        # Some models briefly report stop/idle or empty cycle before explicit dock contact arrives.
        if phase in {"stop", "idle", "ready", "startup_shadow_refresh", ""} and cycle in {"none", "idle", ""} and dock.get("known") is True:
            return True
        return False

    def _derive_vacuum_state(self, live_state: dict[str, Any]) -> str:
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        phase = str(cms.get("phase") or "").lower()
        cycle = str(cms.get("cycle") or "").lower()
        dock = live_state.get("dock") if isinstance(live_state.get("dock"), dict) else {}
        if self._is_definitely_docked(live_state):
            return "docked"
        if phase in {"run", "resume", "new", "explore", "evac", "hmusr", "hmusrdock"}:
            return "cleaning"
        if phase in {"pause", "paused"}:
            return "paused"
        if phase == "stop":
            if cycle == "clean":
                return "idle"
            return "idle"
        if phase in {"hmmidmssn", "return", "returning", "homing", "dockroute", "dockroutecomplete"}:
            return "returning"
        if phase in {"stuck", "cancelled", "error"}:
            return "error"
        if dock.get("known") is True and cycle in {"none", "idle", ""}:
            return "docked"
        return self._restored_data.get("vacuum_state") or "idle"

    def _derive_status_message(self, status: dict[str, Any]) -> str:
        phase = str(status.get("mission_phase") or "").lower()
        cycle = str(status.get("mission_cycle") or "").lower()
        error = status.get("mission_error")
        not_ready = status.get("mission_not_ready")
        bin_present = status.get("bin_present")
        tank_present = status.get("tank_present")
        detected_pad = str(status.get("detected_pad") or "").lower()

        if error not in (None, 0, "0"):
            return f"Roomba needs attention (error {error})"
        if not_ready not in (None, 0, "0"):
            return f"Roomba is not ready ({not_ready})"
        if phase in {"run", "resume", "new", "explore", "evac", "hmusr"}:
            return "Roomba is cleaning"
        if phase in {"pause", "paused"}:
            return "Roomba is paused"
        if phase in {"hmmidmssn", "return", "returning", "homing", "dockroute", "dockroutecomplete", "dock"}:
            return "Roomba is returning to the dock"
        if phase == "stop":
            if cycle == "clean":
                return "Roomba is ready to clean"
            return "Roomba is idle"
        if phase in {"charge", "chargecompleted", "recharge", "hmusrdock", "dockend"} and cycle in {"none", "idle", ""}:
            if bin_present is False:
                return "Roomba is docked, but the bin is missing"
            mop_ready = bool(tank_present) and detected_pad not in {"", "nopad", "none", "unknown", "false", "0"}
            if bin_present and mop_ready:
                return "Roomba is ready to vacuum and mop"
            if bin_present:
                return "Roomba is ready to vacuum, but not mop"
            return "Roomba is docked and charging"
        if cycle in {"none", "idle", ""}:
            return "Roomba is idle"
        return "Roomba status is updating"

    def _normalize_vacuum_state_from_status(self, live_state: dict[str, Any], status: dict[str, Any], derived_state: str | None = None) -> str:
        state = str(derived_state or self._derive_vacuum_state(live_state) or "idle").lower()
        phase = str(status.get("mission_phase") or "").lower()
        cycle = str(status.get("mission_cycle") or "").lower()
        message = str(status.get("status_message") or "").lower()
        dock_known = bool(status.get("dock_known"))
        dock_contact = bool(status.get("dock_contact"))

        idle_like_phase = {"stop", "idle", "ready", "startup_shadow_refresh", "charge", "chargecompleted", "dock", "dockend", "recharge", "hmusrdock", ""}
        idle_like_cycle = {"none", "idle", ""}
        docking_like_phase = {"charge", "chargecompleted", "dock", "dockend", "recharge", "hmusrdock"}
        idle_like_message = any(token in message for token in {"idle", "ready"})
        docked_like_message = any(token in message for token in {"docked", "charging", "at the dock", "at dock", "home"})

        if self._is_definitely_docked(live_state):
            return "docked"

        # On this model the explicit dock flags are often absent after a successful
        # return. When mission cycle has already collapsed to none/idle and the
        # status message is idle-like, prefer the completed/home state over the
        # stale transitional "returning" state.
        if cycle in idle_like_cycle and phase in idle_like_phase:
            if dock_contact or dock_known or docked_like_message:
                return "docked"
            if idle_like_message:
                return "docked"

        if "paused" in message:
            return "paused"
        if "cleaning" in message:
            return "cleaning"
        if "returning" in message or "returning to the dock" in message or "homing" in message:
            return "returning"
        if idle_like_message or docked_like_message:
            if dock_contact or dock_known or docked_like_message or phase in docking_like_phase:
                return "docked"
            if cycle in idle_like_cycle and phase in idle_like_phase:
                return "docked"
            return "idle"
        if "error" in message or "attention" in message:
            return "error"
        return state

    def _build_live_status_block(self, live_state: dict[str, Any], previous_status: dict[str, Any] | None = None) -> dict[str, Any]:
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        stable_pose = self._stabilize_pose(live_state, previous_status)
        signal = live_state.get("signal") if isinstance(live_state.get("signal"), dict) else {}
        dock = live_state.get("dock") if isinstance(live_state.get("dock"), dict) else {}
        bin_state = live_state.get("bin") if isinstance(live_state.get("bin"), dict) else {}
        meta = live_state.get("_meta") if isinstance(live_state.get("_meta"), dict) else {}
        previous_status = previous_status or {}
        status = {
            "battery": live_state.get("batPct"),
            "mission_phase": cms.get("phase"),
            "mission_cycle": cms.get("cycle"),
            "mission_error": cms.get("error"),
            "mission_not_ready": cms.get("notReady"),
            "mission_minutes": cms.get("mssnM"),
            "mission_sqft": cms.get("sqft"),
            "x": stable_pose.get("x"),
            "y": stable_pose.get("y"),
            "theta": stable_pose.get("theta"),
            "pose_source": stable_pose.get("source"),
            "dock_known": dock.get("known"),
            "dock_contact": dock.get("contact"),
            "bin_present": bin_state.get("present"),
            "bin_full": bin_state.get("full"),
            "tank_present": live_state.get("tankPresent"),
            "detected_pad": live_state.get("detectedPad"),
            "operating_mode": self.current_operating_mode_value() if live_state is (self.data or self._restored_data or {}).get("live_state") else (lambda cms, ls: (int(cms.get("operatingMode") or ls.get("operatingMode")) if (cms.get("operatingMode") or ls.get("operatingMode")) is not None else None))(cms, live_state),
            "wifi_rssi": signal.get("rssi"),
            "wifi_snr": signal.get("snr"),
            "last_topic": meta.get("last_topic"),
            "last_update": meta.get("last_update"),
        }
        preserve_keys = {
            "battery", "wifi_rssi", "wifi_snr", "bin_present", "bin_full",
            "tank_present", "detected_pad", "dock_known", "dock_contact",
            "x", "y", "theta",
        }
        for key in preserve_keys:
            if status.get(key) is None and previous_status.get(key) is not None:
                status[key] = previous_status.get(key)
        operating_value = status.get("operating_mode")
        mode_map = {1: "mop", 2: "vacuum", 3: "vacuum_and_mop", 6: "vacuum_and_mop"}
        status["operating_mode_label"] = mode_map.get(operating_value, f"unknown_{operating_value}" if operating_value is not None else None)
        pad_present = str(status.get("detected_pad") or "").strip().lower() not in {"", "nopad", "no_pad", "none", "unknown", "false", "0"}
        if status["operating_mode_label"] == "mop":
            status["cleaning_mode"] = "Mop"
        elif status["operating_mode_label"] == "vacuum_and_mop":
            status["cleaning_mode"] = "Vacuum + Mop"
        elif status["operating_mode_label"] == "vacuum":
            status["cleaning_mode"] = "Vacuum"
        elif status.get("tank_present") and pad_present:
            status["cleaning_mode"] = "Vacuum + Mop"
        else:
            status["cleaning_mode"] = "Vacuum"
        status["status_message"] = self._derive_status_message(status)
        return status

    def _should_preserve_optimistic_state(self, live_state: dict[str, Any]) -> bool:
        command_state = (self.data or self._restored_data or {}).get("_command_optimistic") or {}
        if not isinstance(command_state, dict):
            return False

        command_name = str(command_state.get("command") or "").lower()
        until_text = command_state.get("preserve_until")
        optimistic_phase = str(command_state.get("mission_phase") or "").lower()
        if not command_name or not until_text or not optimistic_phase:
            return False

        try:
            preserve_until = datetime.fromisoformat(str(until_text))
        except (TypeError, ValueError):
            return False

        if preserve_until.tzinfo is None:
            preserve_until = preserve_until.replace(tzinfo=UTC)
        if datetime.now(tz=UTC) >= preserve_until:
            return False

        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        live_phase = str(cms.get("phase") or "").lower()
        live_cycle = str(cms.get("cycle") or "").lower()
        base = self.data or self._restored_data or {}
        previous_status = base.get("status") if isinstance(base.get("status"), dict) else {}
        # Use a freshly merged live status snapshot here, not the previously preserved
        # optimistic status block. Otherwise some models can remain stuck in
        # "returning" even after they are physically docked and reporting
        # idle/startup_shadow_refresh via MQTT.
        status = self._build_live_status_block(live_state, previous_status)
        status_phase = str(status.get("mission_phase") or "").lower()
        status_cycle = str(status.get("mission_cycle") or "").lower()
        status_message = str(status.get("status_message") or "").lower()
        normalized_state = self._normalize_vacuum_state_from_status(live_state, status, command_state.get("vacuum_state") or self._derive_vacuum_state(live_state))

        if command_name == "clean_all" and live_phase == "stop" and live_cycle == "clean":
            return True
        if command_name == "dock":
            # Clear optimistic return state as soon as either live state OR merged status
            # indicates the robot is effectively home/idle again.
            if self._is_definitely_docked(live_state):
                return False
            if normalized_state in {"docked", "idle"}:
                return False
            if live_phase in {"error", "stuck", "cancelled"}:
                return False
            if live_phase in {"stop", "idle", "ready", "startup_shadow_refresh", ""} and live_cycle in {"none", "idle", ""}:
                return False
            if status_cycle in {"none", "idle", ""} and status_phase in {"stop", "idle", "ready", "startup_shadow_refresh", "charge", "chargecompleted", "dock", "dockend", ""}:
                return False
            if "idle" in status_message or "docked" in status_message or "charging" in status_message:
                return False
            return True
        return False

    async def _apply_optimistic_state(
        self,
        vacuum_state: str,
        mission_phase: str | None,
        status_message: str,
        *,
        command_name: str | None = None,
        preserve_seconds: int = 8,
    ) -> None:
        now = datetime.now(tz=UTC)
        base = dict(self.data or self._restored_data or {})
        status = dict(base.get("status") or {})
        if mission_phase is not None:
            status["mission_phase"] = mission_phase
        if command_name == "dock":
            status["mission_cycle"] = status.get("mission_cycle") or "clean"
        status["status_message"] = status_message
        status["last_update"] = now.isoformat()
        if status.get("battery") is None:
            live_state = (base.get("live_state") if isinstance(base.get("live_state"), dict) else {})
            if isinstance(live_state, dict):
                status["battery"] = live_state.get("batPct")
        base["status"] = status
        base["vacuum_state"] = vacuum_state
        if command_name:
            base["_command_optimistic"] = {
                "command": command_name,
                "vacuum_state": vacuum_state,
                "mission_phase": mission_phase,
                "status_message": status_message,
                "preserve_until": (now + timedelta(seconds=preserve_seconds)).isoformat(),
                "issued_at": now.isoformat(),
            }
        self._restored_data = dict(base)
        await self.store.async_save(base)
        self.async_set_updated_data(base)

    async def _write_debug_aliases(self) -> None:
        if not self.debug_enabled or self.debug_dir == self.legacy_debug_dir:
            return
        try:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            self.legacy_debug_dir.mkdir(parents=True, exist_ok=True)
            for src in self.debug_dir.glob("*"):
                if src.is_file():
                    dst = self.legacy_debug_dir / src.name
                    try:
                        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
                    except UnicodeDecodeError:
                        dst.write_bytes(src.read_bytes())
        except Exception as err:
            _LOGGER.debug("roomba_v4 debug alias write failed: %s", err)


    def _current_pose_source(self, live_state: dict[str, Any]) -> tuple[dict[str, Any], str] | tuple[None, None]:
        livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
        current = livemap.get("current") if isinstance(livemap.get("current"), dict) else {}
        if isinstance(current.get("x"), (int, float)) and isinstance(current.get("y"), (int, float)):
            return ({"x": float(current.get("x")), "y": float(current.get("y")), "theta": current.get("theta")}, "livemap_current")
        path_points = livemap.get("path_points") if isinstance(livemap.get("path_points"), list) else []
        for pt in reversed(path_points):
            if isinstance(pt, dict) and isinstance(pt.get("x"), (int, float)) and isinstance(pt.get("y"), (int, float)):
                return ({"x": float(pt.get("x")), "y": float(pt.get("y")), "theta": pt.get("theta")}, "livemap_path")
        pose = live_state.get("pose") if isinstance(live_state.get("pose"), dict) else {}
        point = pose.get("point") if isinstance(pose.get("point"), dict) else {}
        if isinstance(point.get("x"), (int, float)) or isinstance(point.get("y"), (int, float)):
            return ({"x": point.get("x"), "y": point.get("y"), "theta": pose.get("theta")}, "pose")
        return (None, None)

    def _stabilize_pose(self, live_state: dict[str, Any], previous_status: dict[str, Any] | None = None) -> dict[str, Any]:
        previous_status = previous_status or {}
        candidate, source = self._current_pose_source(live_state)
        last_good = self._last_good_pose if isinstance(self._last_good_pose, dict) else None
        prev_x = previous_status.get("x")
        prev_y = previous_status.get("y")
        prev_theta = previous_status.get("theta")

        if candidate is None:
            return {"x": prev_x, "y": prev_y, "theta": prev_theta, "source": "previous" if prev_x is not None or prev_y is not None else None}

        x = candidate.get("x")
        y = candidate.get("y")
        theta = candidate.get("theta")
        if x is None and prev_x is not None:
            x = prev_x
        if y is None and prev_y is not None:
            y = prev_y
        if theta is None and prev_theta is not None:
            theta = prev_theta

        if last_good and all(isinstance(candidate.get(k), (int, float)) for k in ("x", "y")) and all(isinstance(last_good.get(k), (int, float)) for k in ("x", "y")):
            dx = float(candidate.get("x")) - float(last_good.get("x"))
            dy = float(candidate.get("y")) - float(last_good.get("y"))
            distance = (dx * dx + dy * dy) ** 0.5
            moving = self._mission_is_actively_moving(live_state)
            if moving and source == "pose" and distance > 0.75:
                return {
                    "x": last_good.get("x"),
                    "y": last_good.get("y"),
                    "theta": last_good.get("theta") if last_good.get("theta") is not None else theta,
                    "source": "rejected_pose_jump",
                }

        stable = {"x": x, "y": y, "theta": theta, "source": source}
        if isinstance(stable.get("x"), (int, float)) and isinstance(stable.get("y"), (int, float)):
            self._last_good_pose = {"x": float(stable.get("x")), "y": float(stable.get("y")), "theta": stable.get("theta"), "source": source}
        return stable

    def _livemap_pose_present(self, live_state: dict[str, Any]) -> bool:
        livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
        current = livemap.get("current") if isinstance(livemap.get("current"), dict) else {}
        if any(current.get(k) is not None for k in ("x", "y", "theta")):
            return True
        pose = live_state.get("pose") if isinstance(live_state.get("pose"), dict) else {}
        point = pose.get("point") if isinstance(pose.get("point"), dict) else {}
        return any(point.get(k) is not None for k in ("x", "y")) or pose.get("theta") is not None

    def _is_livemap_topic(self, live_state: dict[str, Any]) -> bool:
        meta = live_state.get("_meta") if isinstance(live_state.get("_meta"), dict) else {}
        topic = str(meta.get("last_topic") or "")
        return "/livemap/" in topic

    def _mission_is_actively_moving(self, live_state: dict[str, Any]) -> bool:
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        phase = str(cms.get("phase") or "").lower()
        cycle = str(cms.get("cycle") or "").lower()
        if cycle not in {"clean", "quick", "spot", "explore", "train", "vacuum"}:
            return False
        return phase in {"run", "resume", "hmusrdock", "recharge", "hm_mid_msn", "pause"}

    def _should_trigger_livemap(self, live_state: dict[str, Any]) -> bool:
        if not self._mission_is_actively_moving(live_state):
            return False
        now = datetime.now(tz=UTC)
        if self._last_livemap_message_at and (now - self._last_livemap_message_at) < timedelta(seconds=20):
            return False
        if self._livemap_pose_present(live_state):
            return False
        if self._last_livemap_trigger_at and (now - self._last_livemap_trigger_at) < timedelta(seconds=15):
            return False
        return True

    async def _trigger_livemap_stream(self, reason: str, live_state: dict[str, Any]) -> None:
        self._last_livemap_trigger_at = datetime.now(tz=UTC)
        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        try:
            mqtt_topic = await self.api.get_livemap_mqtt_topic(self.robot_blid)
            await self.api._write_runtime_debug("livemap_trigger", {
                "robot_id": self.robot_blid,
                "reason": reason,
                "mission_phase": cms.get("phase"),
                "mission_cycle": cms.get("cycle"),
                "mqtt_topic": mqtt_topic,
                "subscribed_topics": sorted(getattr(self.api, "_subscriber_topics_subscribed", set())),
                "ts": datetime.now(tz=UTC).isoformat(),
                "mode": "v208_livemap_trigger_on_active_mission",
            })
            if mqtt_topic and mqtt_topic not in getattr(self.api, "_subscriber_topics_subscribed", set()):
                await self.api.async_ensure_event_subscriber(self.robot_blid)
        except Exception as err:
            _LOGGER.debug("roomba_v4 livemap trigger failed: %s", err, exc_info=True)
            try:
                await self.api._write_runtime_debug("livemap_trigger_error", {
                    "robot_id": self.robot_blid,
                    "reason": reason,
                    "error": str(err),
                    "ts": datetime.now(tz=UTC).isoformat(),
                    "mode": "v208_livemap_trigger_on_active_mission",
                })
            except Exception:
                pass


    def _append_pose_to_cumulative_path(self, livemap: dict[str, Any], pose: dict[str, Any] | None, live_state: dict[str, Any], vacuum_state: str | None = None, status: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(livemap, dict) or not isinstance(pose, dict):
            return livemap
        x = pose.get("x")
        y = pose.get("y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            return livemap
        if self._is_definitely_docked(live_state):
            return livemap

        cms = live_state.get("cleanMissionStatus") if isinstance(live_state.get("cleanMissionStatus"), dict) else {}
        phase = str(cms.get("phase") or (status or {}).get("mission_phase") or "").lower()
        cycle = str(cms.get("cycle") or (status or {}).get("mission_cycle") or "").lower()
        vacuum_state = str(vacuum_state or "").lower()

        # Some 105 firmware/live topics do not consistently populate cycle/phase while the robot is moving.
        # Be permissive here: record a synthetic trail whenever the robot is not definitely docked and the
        # state looks active enough that position updates are meaningful.
        if vacuum_state in {"docked", "charging", "idle", "ready", "unknown", "error"} and cycle in {"", "none", "idle"} and phase in {"", "charge", "chargecompleted", "stop", "dockend", "dock"}:
            return livemap

        candidate = {
            "x": float(x),
            "y": float(y),
            "theta": pose.get("theta"),
            "flag": "synthetic_pose",
        }
        cumulative = livemap.get("cumulative_path_points") if isinstance(livemap.get("cumulative_path_points"), list) else []
        merged: list[dict[str, Any]] = []
        for pt in cumulative:
            if isinstance(pt, dict) and isinstance(pt.get("x"), (int, float)) and isinstance(pt.get("y"), (int, float)):
                merged.append({
                    "x": float(pt.get("x")),
                    "y": float(pt.get("y")),
                    "theta": pt.get("theta"),
                    "flag": pt.get("flag"),
                })

        last = merged[-1] if merged else (self._last_path_pose if isinstance(self._last_path_pose, dict) else None)
        if isinstance(last, dict) and all(isinstance(last.get(k), (int, float)) for k in ("x", "y")):
            dx = float(candidate["x"]) - float(last["x"])
            dy = float(candidate["y"]) - float(last["y"])
            dist = (dx * dx + dy * dy) ** 0.5
            if dist < 0.02:
                return livemap
            if dist > 1.75:
                return livemap

        merged.append(candidate)
        livemap["cumulative_path_points"] = merged[-2500:]
        livemap["cumulative_path_points_count"] = len(livemap["cumulative_path_points"])
        self._last_path_pose = {"x": candidate["x"], "y": candidate["y"], "theta": candidate.get("theta")}
        return livemap

    def _merge_live_state(self, previous: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        merged = dict(previous or {})
        for key, value in (incoming or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_live_state(merged.get(key) or {}, value)
            else:
                merged[key] = value
        return merged

    async def _handle_live_state_update(self, live_state: dict[str, Any]) -> None:
        if self._is_livemap_topic(live_state):
            self._last_livemap_message_at = datetime.now(tz=UTC)
        base = dict(self.data or self._restored_data or {})

        prev_live_state = base.get("live_state") if isinstance(base.get("live_state"), dict) else {}
        live_state = self._merge_live_state(prev_live_state, live_state)
        prev_livemap = prev_live_state.get("livemap") if isinstance(prev_live_state.get("livemap"), dict) else {}
        cur_livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else None
        if cur_livemap is not None:
            incoming = cur_livemap.get("path_points") if isinstance(cur_livemap.get("path_points"), list) else []
            cumulative = prev_livemap.get("cumulative_path_points") if isinstance(prev_livemap.get("cumulative_path_points"), list) else []
            merged = []
            seen = set()
            for pt in [*cumulative, *incoming]:
                if not isinstance(pt, dict):
                    continue
                x = pt.get("x")
                y = pt.get("y")
                if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
                    continue
                key = (round(float(x), 4), round(float(y), 4))
                if key in seen:
                    continue
                seen.add(key)
                merged.append({
                    "x": float(x),
                    "y": float(y),
                    "theta": pt.get("theta"),
                    "flag": pt.get("flag"),
                })
            cur_livemap["cumulative_path_points"] = merged[-2000:]
            cur_livemap["cumulative_path_points_count"] = len(merged)
            live_state["livemap"] = cur_livemap

        base["live_state"] = live_state
        current_status = base.get("status") or {}
        synthetic_pose = self._stabilize_pose(live_state, current_status)
        if self._should_preserve_optimistic_state(live_state):
            command_state = base.get("_command_optimistic") or {}
            status = self._build_live_status_block(live_state, base.get("status") or {})
            status["last_topic"] = ((live_state.get("_meta") or {}).get("last_topic") if isinstance(live_state.get("_meta"), dict) else None)
            status["last_update"] = datetime.now(tz=UTC).isoformat()
            vacuum_state = self._normalize_vacuum_state_from_status(live_state, status, command_state.get("vacuum_state") or base.get("vacuum_state"))
            status["vacuum_state"] = vacuum_state
            base["status"] = status
            base["vacuum_state"] = vacuum_state
        else:
            status = self._build_live_status_block(live_state, base.get("status") or {})
            vacuum_state = self._normalize_vacuum_state_from_status(live_state, status)
            status["vacuum_state"] = vacuum_state
            base["status"] = status
            base["vacuum_state"] = vacuum_state
            base.pop("_command_optimistic", None)

        await self._maybe_emit_status_events(current_status if isinstance(current_status, dict) else {}, base.get("status") or {}, live_state)

        updated_livemap = base.get("live_state", {}).get("livemap") if isinstance(base.get("live_state", {}).get("livemap"), dict) else None
        if updated_livemap is not None:
            effective_pose = {
                "x": base.get("status", {}).get("x"),
                "y": base.get("status", {}).get("y"),
                "theta": base.get("status", {}).get("theta"),
            }
            base["live_state"]["livemap"] = self._append_pose_to_cumulative_path(
                updated_livemap,
                effective_pose,
                live_state,
                base.get("vacuum_state"),
                base.get("status") or {},
            )
        self._restored_data = dict(base)
        await self.store.async_save(base)
        self._apply_update_interval(base.get("vacuum_state"))
        self.async_set_updated_data(base)
        await self._write_debug_json("live_status_block.json", base.get("status") or {})
        await self._write_debug_aliases()
        if self._should_trigger_livemap(live_state):
            if self._livemap_trigger_task is None or self._livemap_trigger_task.done():
                self._livemap_trigger_task = self.hass.async_create_task(
                    self._trigger_livemap_stream("active_mission_without_recent_livemap", live_state)
                )

    async def async_start_background_subscriber(self) -> None:
        """Start the cloud subscriber independently of user actions."""
        if self._subscriber_boot_task and not self._subscriber_boot_task.done():
            return

        async def _runner() -> None:
            try:
                self.api.debug_dir = self.debug_dir if self.debug_enabled else None
                await self.api._write_runtime_debug("mqtt_task_scheduled", {
                    "robot_id": self.robot_blid,
                    "entry_id": self.entry_id,
                    "source": "coordinator_background_start",
                    "ts": datetime.now(tz=UTC).isoformat(),
                })
                if not self.api.robots:
                    await self.api.authenticate()
                await self.api.async_ensure_event_subscriber(self.robot_blid)
            except Exception as err:
                _LOGGER.debug("roomba_v4 background subscriber start failed: %s", err, exc_info=True)
                try:
                    await self.api._write_runtime_debug("mqtt_task_error", {
                        "robot_id": self.robot_blid,
                        "entry_id": self.entry_id,
                        "source": "coordinator_background_start",
                        "error": str(err),
                        "ts": datetime.now(tz=UTC).isoformat(),
                    })
                except Exception:
                    pass

        self._subscriber_boot_task = self.hass.async_create_task(_runner())

    async def _async_update_data(self) -> dict:
        try:
            await self._restore_state_once()

            if not self.api.robots:
                await self.api.authenticate()

            robot = self.api.robots.get(self.robot_blid)
            if not robot:
                raise UpdateFailed(f"Robot {self.robot_blid} not found")

            self.api.debug_dir = self.debug_dir if self.debug_enabled else None
            if self.debug_enabled:
                self.legacy_debug_dir.mkdir(parents=True, exist_ok=True)
            await self.async_start_background_subscriber()

            pmaps: Any = []
            try:
                pmaps = await self.api.get_pmaps(self.robot_blid)
            except Exception as err:
                _LOGGER.debug("roomba_v4 debug: pmaps fetch failed: %s", err)

            favorites: Any = {}
            try:
                favorites = await self.api.get_favorites()
            except Exception as err:
                _LOGGER.debug("roomba_v4 debug: favorites fetch failed: %s", err)

            schedules: Any = {}
            try:
                schedules = await self.api.get_schedules()
            except Exception as err:
                _LOGGER.debug("roomba_v4 debug: schedules fetch failed: %s", err)

            mission_history = await self.api.get_mission_history(self.robot_blid)

            active_map = self._select_active_map(robot, pmaps, mission_history)
            mission_map = self._latest_mission_with_p2map(mission_history)
            active_map_id = self._resolve_active_map_id(robot, active_map, mission_map)
            active_map_version = self._resolve_active_map_version(robot, active_map, mission_map)

            # These newer room-metadata endpoints are optional and may not be authorized
            # for every account/robot. They must never block setup.
            p2map_clean_score: dict[str, Any] | None = None
            p2map_routines: list[dict[str, Any]] = []
            if active_map_id:
                try:
                    p2map_clean_score = await self.api.get_p2map_clean_score(active_map_id)
                except Exception as err:
                    _LOGGER.debug("roomba_v4 debug: clean score fetch failed: %s", err)
                try:
                    routines = await self.api.get_p2map_routines(active_map_id)
                    p2map_routines = routines if isinstance(routines, list) else []
                except Exception as err:
                    _LOGGER.debug("roomba_v4 debug: routines fetch failed: %s", err)

            await self._write_debug_json("robot.json", robot)
            await self._write_debug_json("pmaps.json", pmaps)
            await self._write_debug_json("mission_history.json", mission_history)
            await self._write_debug_json("favorites.json", favorites)
            await self._write_debug_json("schedules.json", schedules)
            await self._write_debug_json("active_map.json", active_map)
            await self._write_debug_json("latest_mission_with_p2map.json", mission_map)
            await self._write_debug_json("map_id_candidates.json", self._collect_map_id_candidates(robot, active_map, mission_history))
            await self._write_debug_json("clean_score.json", p2map_clean_score or {})
            await self._write_debug_json("routines.json", p2map_routines)

            transport_debug = self.api.get_cloud_transport_debug_info(self.robot_blid)
            await self._write_debug_json("cloud_transport_debug.json", transport_debug)

            effective_url = self.s3_map_url or self._extract_map_url(robot, active_map, mission_history)
            url_candidates = self._deep_find_candidates({"robot": robot, "active_map": active_map, "mission_history": mission_history, "pmaps": pmaps})
            await self._write_debug_json("url_candidates.json", url_candidates)
            if effective_url:
                await self._write_debug_text("extracted_map_url.txt", effective_url)

            map_download_state: dict[str, Any] = {
                "attempted": False,
                "method": None,
                "url": effective_url,
                "p2map_id": active_map_id,
                "p2mapv_id": active_map_version,
            }

            should_try_map = self.auto_download_map and bool(active_map_id and active_map_version)
            last_downloaded_id = self._restored_data.get("last_downloaded_map_id")
            last_downloaded_version = self._restored_data.get("last_downloaded_map_version")
            map_changed = (active_map_id != last_downloaded_id) or (active_map_version != last_downloaded_version)

            if should_try_map and (map_changed or self.map_png_bytes is None or not self.room_info):
                try:
                    map_download_state["attempted"] = True
                    if effective_url:
                        map_download_state["method"] = "direct_url"
                        try:
                            await self.async_download_and_render_map(effective_url)
                        except Exception as direct_err:
                            map_download_state["direct_url_error"] = str(direct_err)
                            map_download_state["method"] = "p2maps_binary_endpoint"
                            await self.async_download_and_render_map_from_p2map(active_map_id, active_map_version)
                    else:
                        map_download_state["method"] = "p2maps_binary_endpoint"
                        await self.async_download_and_render_map_from_p2map(active_map_id, active_map_version)
                    self.s3_map_url = effective_url
                    self._restored_data["last_downloaded_map_id"] = active_map_id
                    self._restored_data["last_downloaded_map_version"] = active_map_version
                except Exception as err:
                    map_download_state["error"] = str(err)
                    await self._write_debug_json("map_download_error.json", map_download_state)
                    _LOGGER.warning("roomba_v4 debug: map download/render failed: %s", err)
            elif self.auto_download_map and not should_try_map:
                map_download_state["skipped"] = True
                map_download_state["reason"] = "missing_map_id_or_version"

            await self._write_debug_json("map_download_state.json", map_download_state)

            self._apply_room_metadata(active_map, p2map_clean_score)
            await self._write_debug_json("resolved_room_info.json", self.room_info)

            live_state = self.api.get_live_state_snapshot()
            status_block = self._build_live_status_block(live_state)
            vacuum_state = self._normalize_vacuum_state_from_status(live_state, status_block)
            status_block["vacuum_state"] = vacuum_state
            await self._write_debug_json("live_state_snapshot.json", live_state)
            await self._write_debug_json("live_status_block.json", status_block)

            summary = {
                "active_map_id": active_map_id,
                "active_map_version": active_map_version,
                "s3_map_url_known": bool(effective_url) or bool(active_map_id and active_map_version),
                "room_count": len(self.rooms),
                "live_state_keys": sorted(live_state.keys()),
                "live_status": status_block,
                "routines_count": len(p2map_routines),
                "map_png_ready": self.map_png_bytes is not None,
                "map_download_attempted": bool(map_download_state.get("attempted")),
                "map_download_method": map_download_state.get("method"),
                "livemap_topic": self.api._livemap_topics.get(self.robot_blid),
                "pmaps_type": type(pmaps).__name__,
                "pmaps_count": len(pmaps) if isinstance(pmaps, list) else None,
                "mission_entry_count": len(self._find_mission_dicts(mission_history)),
            }
            await self._write_debug_json("summary.json", summary)

            data = {
                "robot": robot,
                "pmaps": pmaps,
                "active_map": active_map,
                "active_map_id": active_map_id,
                "active_map_version": active_map_version,
                "favorites": favorites,
                "schedules": schedules,
                "mission_history": mission_history,
                "latest_mission_with_p2map": mission_map,
                "map_png_ready": self.map_png_bytes is not None,
                "last_map_refresh": self.last_map_refresh,
                "s3_map_url_known": bool(effective_url) or bool(active_map_id and active_map_version),
                "s3_map_url": effective_url,
                "rooms": self.rooms,
                "room_info": self.room_info,
            "map_render_metadata": self.map_render_metadata,
                "selected_room": self.selected_room,
                "preferred_cleaning_mode": self.preferred_cleaning_mode(),
                "preferred_suction_level": self.preferred_suction_level(),
                "preferred_water_level": self.preferred_water_level(),
                "live_state": live_state,
                "status": status_block,
                "vacuum_state": vacuum_state,
                "clean_score": p2map_clean_score,
                "routines": p2map_routines,
                "debug_dir": str(self.debug_dir),
                "last_downloaded_map_id": self._restored_data.get("last_downloaded_map_id"),
                "last_downloaded_map_version": self._restored_data.get("last_downloaded_map_version"),
            }
            await self.store.async_save(data)
            self._restored_data = data
            self._apply_update_interval(vacuum_state)
            await self._write_debug_aliases()
            return data
        except CloudApiError as err:
            raise UpdateFailed(str(err)) from err

    async def async_shutdown(self) -> None:
        self.api.remove_live_state_listener(self._handle_live_state_update)

    async def _restore_state_once(self) -> None:
        if self._restored:
            return
        self._restored = True
        restored = await self.store.async_load() or {}
        self._restored_data = restored if isinstance(restored, dict) else {}
        self.last_map_refresh = self._restored_data.get("last_map_refresh")
        if isinstance(self._restored_data.get("live_state"), dict):
            self.api._live_state = dict(self._restored_data.get("live_state") or {})
        self.room_info = list(self._restored_data.get("room_info") or [])
        self.rooms = room_names_from_info(self.room_info) or list(self._restored_data.get("rooms") or [])
        self.map_render_metadata = dict(self._restored_data.get("map_render_metadata") or {})
        self.selected_room = self._restored_data.get("selected_room")
        if self.supports_mopping() and not self._restored_data.get("preferred_cleaning_mode"):
            self._restored_data["preferred_cleaning_mode"] = self.derived_cleaning_mode()
        stored_url = self._restored_data.get("s3_map_url")
        if not self.s3_map_url and isinstance(stored_url, str) and stored_url:
            self.s3_map_url = stored_url

    def _iter_nodes(self, obj: Any, path: str = "$"):
        yield path, obj
        if isinstance(obj, dict):
            for key, value in obj.items():
                yield from self._iter_nodes(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for idx, value in enumerate(obj):
                yield from self._iter_nodes(value, f"{path}[{idx}]")

    def _iter_dict_nodes(self, obj: Any, path: str = "$"):
        for node_path, node in self._iter_nodes(obj, path):
            if isinstance(node, dict):
                yield node_path, node

    def _select_active_map(self, robot: dict[str, Any], pmaps: Any, mission_history: Any) -> dict[str, Any] | None:
        if isinstance(pmaps, list):
            for pmap in pmaps:
                if isinstance(pmap, dict) and (
                    pmap.get("active_p2mapv_id")
                    or pmap.get("active_pmapv_id")
                    or pmap.get("state") == "active"
                    or pmap.get("visible") is True
                    or pmap.get("active")
                    or pmap.get("active_map")
                ):
                    return pmap
            first = next((p for p in pmaps if isinstance(p, dict)), None)
            if first:
                return first
        elif isinstance(pmaps, dict):
            if any(key in pmaps for key in MAP_ID_KEYS + MAP_VERSION_KEYS):
                return pmaps
            for _, node in self._iter_dict_nodes(pmaps):
                if any(key in node for key in MAP_ID_KEYS + MAP_VERSION_KEYS):
                    return node
        for _, node in self._iter_dict_nodes(robot):
            if any(key in node for key in MAP_ID_KEYS + MAP_VERSION_KEYS):
                return node
        mission = self._latest_mission_with_p2map(mission_history)
        return mission

    def _find_mission_dicts(self, mission_history: Any) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for _, node in self._iter_dict_nodes(mission_history):
            keys = set(node.keys())
            if {"p2map_id", "p2mapv_id"}.issubset(keys) or "missionId" in keys or "done" in keys or "cmd" in keys:
                matches.append(node)
        # preserve order, remove duplicates by object id
        seen: set[int] = set()
        unique: list[dict[str, Any]] = []
        for node in matches:
            node_id = id(node)
            if node_id not in seen:
                seen.add(node_id)
                unique.append(node)
        return unique

    def _latest_mission_with_p2map(self, mission_history: Any) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for item in self._find_mission_dicts(mission_history):
            if item.get("p2map_id") and item.get("p2mapv_id"):
                candidates.append(item)
        if not candidates:
            for _, node in self._iter_dict_nodes(mission_history):
                if node.get("p2map_id") and node.get("p2mapv_id"):
                    candidates.append(node)
        if not candidates:
            return None

        def sort_key(item: dict[str, Any]) -> tuple[str, str]:
            return (str(item.get("p2map_id") or ""), str(item.get("p2mapv_id") or ""))

        return max(candidates, key=sort_key)

    def _first_value(self, *sources: Any, keys: tuple[str, ...]) -> str | None:
        for source in sources:
            if not source:
                continue
            if isinstance(source, dict):
                for key in keys:
                    value = source.get(key)
                    if value not in (None, ""):
                        return str(value)
            for _, node in self._iter_dict_nodes(source):
                for key in keys:
                    value = node.get(key)
                    if value not in (None, ""):
                        return str(value)
        return None

    def _resolve_active_map_id(self, robot: dict[str, Any], active_map: dict[str, Any] | None, mission_map: dict[str, Any] | None) -> str | None:
        current = self._first_value(active_map, robot, mission_map, keys=MAP_ID_KEYS)
        if current:
            return current
        restored = self._restored_data.get("active_map_id")
        return str(restored) if restored else None

    def _resolve_active_map_version(self, robot: dict[str, Any], active_map: dict[str, Any] | None, mission_map: dict[str, Any] | None) -> str | None:
        current = self._first_value(active_map, robot, mission_map, keys=MAP_VERSION_KEYS)
        if current:
            return current
        restored = self._restored_data.get("active_map_version")
        return str(restored) if restored else None

    def _looks_like_mission_history_map_id(self, map_id: str) -> bool:
        parts = map_id.split("-", 1)
        return len(parts) == 2 and parts[0] == self.robot_blid and parts[1].isdigit()

    def _collect_map_id_candidates(self, robot: Any, active_map: Any, mission_history: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        scanned = {"robot": robot, "active_map": active_map, "mission_history": mission_history}
        for root_name, root_obj in scanned.items():
            for path, node in self._iter_dict_nodes(root_obj, f"$.{root_name}"):
                hit: dict[str, Any] = {"path": path}
                found = False
                for key in MAP_ID_KEYS + MAP_VERSION_KEYS:
                    if key in node and node.get(key) not in (None, ""):
                        hit[key] = node.get(key)
                        found = True
                if found:
                    out.append(hit)
        return out


    def _extract_rooms_metadata(self, active_map: Any) -> list[dict[str, Any]]:
        if isinstance(active_map, dict):
            rooms_metadata = active_map.get("rooms_metadata")
            if isinstance(rooms_metadata, list):
                return [item for item in rooms_metadata if isinstance(item, dict)]
        return []

    def _numeric_room_id(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            s = str(value).strip()
            return int(s) if s.isdigit() else None
        except Exception:
            return None

    def _set_room_control_id(self, room: dict[str, Any], control_id: Any) -> bool:
        if not isinstance(room, dict) or control_id is None:
            return False
        control_id_str = str(control_id)
        changed = room.get("control_room_id") != control_id_str
        room["control_room_id"] = control_id_str
        room.setdefault("room_id", control_id_str)
        props = room.setdefault("properties", {})
        if isinstance(props, dict):
            if props.get("control_room_id") != control_id_str:
                props["control_room_id"] = control_id_str
                changed = True
            props.setdefault("room_id", control_id_str)
            props.setdefault("segment_id", control_id_str)
            props.setdefault("region_id", control_id_str)
        return changed

    def _generic_room_name(self, name: str | None) -> bool:
        if not name:
            return True
        low = str(name).strip().lower()
        return low.startswith("room ") or low in {"unknown", "none"}

    def _apply_room_metadata(self, active_map: Any, clean_score: Any = None) -> None:
        metadata_items = self._extract_rooms_metadata(active_map)
        if not metadata_items:
            self.rooms = room_names_from_info(self.room_info)
            return

        by_id: dict[str, str] = {}
        numeric_pairs: list[tuple[int, str]] = []
        for item in metadata_items:
            room_id = item.get("room_id")
            meta = item.get("room_metadata") or {}
            name = meta.get("name") or meta.get("title") or meta.get("label")
            if room_id is not None and name:
                room_id_str = str(room_id)
                room_name = str(name)
                by_id[room_id_str] = room_name
                numeric_id = self._numeric_room_id(room_id)
                if numeric_id is not None:
                    numeric_pairs.append((numeric_id, room_name))

        changed = False
        for room in self.room_info:
            props = room.get("properties") or {}
            candidate_ids = [
                room.get("id"),
                room.get("room_id"),
                props.get("id"),
                props.get("room_id"),
                props.get("region_id"),
                props.get("segment_id"),
                props.get("regionId"),
                props.get("feature_id"),
                props.get("featureId"),
            ]
            override = None
            for value in candidate_ids:
                if value is None:
                    continue
                override = by_id.get(str(value))
                if override:
                    break
            if override and room.get("name") != override:
                room["name"] = override
                changed = True
            if override:
                for value in candidate_ids:
                    if value is None:
                        continue
                    if by_id.get(str(value)) == override:
                        changed = self._set_room_control_id(room, value) or changed
                        break

        # Heuristic for archives that only expose synthetic room ids like 1..N while
        # the cloud metadata uses numeric ids offset into a larger range such as 10..19.
        generic_rooms = [room for room in self.room_info if self._generic_room_name(room.get("name"))]
        if generic_rooms and numeric_pairs:
            numeric_pairs = sorted(numeric_pairs, key=lambda item: item[0])
            room_numeric_ids = [self._numeric_room_id((room.get("properties") or {}).get("id") or room.get("id")) for room in generic_rooms]
            room_numeric_ids = [value for value in room_numeric_ids if value is not None]
            if room_numeric_ids:
                min_meta = numeric_pairs[0][0]
                min_room = min(room_numeric_ids)
                offset = min_meta - min_room
                numeric_name_map = {meta_id - offset: name for meta_id, name in numeric_pairs}
                for room in generic_rooms:
                    props = room.get("properties") or {}
                    numeric_id = self._numeric_room_id(props.get("id") or room.get("id"))
                    if numeric_id is None:
                        continue
                    override = numeric_name_map.get(numeric_id)
                    if override:
                        room["name"] = override
                        changed = True
                        for meta_id, meta_name in numeric_pairs:
                            if meta_name == override:
                                changed = self._set_room_control_id(room, meta_id) or changed
                                break

        if len(self.room_info) == len(metadata_items) and all(self._generic_room_name(r.get("name")) for r in self.room_info):
            ordered_names = [by_id.get(str(item.get("room_id"))) for item in metadata_items]
            ordered_names = [name for name in ordered_names if name]
            if len(ordered_names) == len(self.room_info):
                for room, item, override in zip(self.room_info, metadata_items, ordered_names, strict=False):
                    room["name"] = override
                    changed = self._set_room_control_id(room, item.get("room_id")) or changed
                changed = True

        # Fallback: when only a subset of rooms have cloud names, preserve the mapped names
        # and keep the remaining generic archive names.
        self.rooms = room_names_from_info(self.room_info) or [by_id[key] for key in by_id]
        if self.rooms and self.selected_room not in self.rooms:
            self.selected_room = self.rooms[0]

    async def _write_debug_json(self, filename: str, payload: Any) -> None:
        if not self.debug_enabled:
            return
        try:
            await self.hass.async_add_executor_job(self._write_text_file, filename, json.dumps(payload, indent=2, default=str))
        except Exception as err:
            _LOGGER.warning("roomba_v4 debug: failed writing %s: %s", filename, err)

    async def _write_debug_text(self, filename: str, text: str) -> None:
        if not self.debug_enabled:
            return
        try:
            await self.hass.async_add_executor_job(self._write_text_file, filename, text)
        except Exception as err:
            _LOGGER.warning("roomba_v4 debug: failed writing %s: %s", filename, err)

    def _write_text_file(self, filename: str, text: str) -> None:
        if not self.debug_enabled:
            return
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        (self.debug_dir / filename).write_text(text, encoding="utf-8")

    def _deep_find_candidates(self, obj: Any, out: list[dict[str, str]] | None = None, path: str = "$") -> list[dict[str, str]]:
        if out is None:
            out = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                subpath = f"{path}.{key}"
                if isinstance(value, str):
                    low = value.lower()
                    if any(token in low for token in URL_HINT_TOKENS):
                        out.append({"path": subpath, "value": value})
                else:
                    self._deep_find_candidates(value, out, subpath)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                self._deep_find_candidates(item, out, f"{path}[{index}]")
        return out

    def _looks_like_downloadable_map_url(self, value: str, *, key_hint: str | None = None, allow_api_probe_urls: bool = False) -> bool:
        if not value.startswith(("http://", "https://")):
            return False
        parsed = urlparse(value)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = parse_qs(parsed.query)
        key_hint = (key_hint or "").lower()

        if any(token in path for token in ("p2mapv_geojson.tgz", "geojson.tgz", ".geojson", ".tgz")):
            return True
        if "amazonaws" in host and any(k.lower().startswith("x-amz-") for k in query):
            return True
        if key_hint in DIRECT_URL_KEYS and any(token in value.lower() for token in ("geojson", ".tgz", "amazonaws")):
            return True
        if allow_api_probe_urls and any(token in path for token in ("/archive", "/download", "/geojson", "/files", "/manifest")):
            return True
        return False

    def _deep_find_url(self, obj: Any, *, allow_api_probe_urls: bool = False) -> str | None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and self._looks_like_downloadable_map_url(value, key_hint=key, allow_api_probe_urls=allow_api_probe_urls):
                    return value
                found = self._deep_find_url(value, allow_api_probe_urls=allow_api_probe_urls)
                if found:
                    return found
            return None
        if isinstance(obj, list):
            for item in obj:
                found = self._deep_find_url(item, allow_api_probe_urls=allow_api_probe_urls)
                if found:
                    return found
        return None

    def _extract_map_url(self, robot: Any, active_map: dict | None, mission_history: Any) -> str | None:
        for source in (active_map, robot, mission_history):
            if not source:
                continue
            if isinstance(source, dict):
                for key in DIRECT_URL_KEYS:
                    value = source.get(key)
                    if isinstance(value, str) and self._looks_like_downloadable_map_url(value, key_hint=key):
                        return value
            for _, node in self._iter_dict_nodes(source):
                for key in DIRECT_URL_KEYS:
                    value = node.get(key)
                    if isinstance(value, str) and self._looks_like_downloadable_map_url(value, key_hint=key):
                        return value
                found = self._deep_find_url(node)
                if found:
                    return found
        restored = self._restored_data.get("s3_map_url")
        if isinstance(restored, str) and restored.startswith(("http://", "https://")):
            return restored
        return None


    async def _refresh_render_metadata(self, archive: Path) -> None:
        try:
            self.map_render_metadata = await self.hass.async_add_executor_job(
                extract_map_render_metadata, archive, self.map_png_bytes
            )
        except Exception as err:
            _LOGGER.debug("roomba_v4 map render metadata extraction failed: %s", err, exc_info=True)
            self.map_render_metadata = {}

    async def async_download_and_render_map(self, url: str) -> None:
        content = await self.api.download_file(url)
        archive = Path(self.map_archive_path)
        await self.hass.async_add_executor_job(self._write_binary_file, archive, content)
        self.map_png_bytes = await self.hass.async_add_executor_job(render_archive_to_png_bytes, archive, True, True)
        self.room_info = await self.hass.async_add_executor_job(extract_room_info_from_archive, archive)
        self.rooms = room_names_from_info(self.room_info)
        await self._refresh_render_metadata(archive)
        if self.rooms and self.selected_room not in self.rooms:
            self.selected_room = self.rooms[0]
        self.last_map_refresh = datetime.now(tz=UTC).isoformat()
        self.async_update_listeners()

    async def async_download_and_render_map_from_p2map(self, p2map_id: str, p2mapv_id: str) -> None:
        content = await self.api.download_p2map_geojson(p2map_id, p2mapv_id)
        archive = Path(self.map_archive_path)
        await self.hass.async_add_executor_job(self._write_binary_file, archive, content)
        self.map_png_bytes = await self.hass.async_add_executor_job(render_archive_to_png_bytes, archive, True, True)
        self.room_info = await self.hass.async_add_executor_job(extract_room_info_from_archive, archive)
        self.rooms = room_names_from_info(self.room_info)
        await self._refresh_render_metadata(archive)
        if self.rooms and self.selected_room not in self.rooms:
            self.selected_room = self.rooms[0]
        self.last_map_refresh = datetime.now(tz=UTC).isoformat()
        self.async_update_listeners()


    async def async_delete_cached_maps_and_fetch_latest(self) -> None:
        archive = Path(self.map_archive_path)
        await self.hass.async_add_executor_job(self._delete_cached_map_files, archive)
        self.map_png_bytes = None
        self.room_info = []
        self.rooms = []
        self.map_render_metadata = {}
        self.selected_room = None
        self.last_map_refresh = None
        self.s3_map_url = None
        self._restored_data.pop("s3_map_url", None)
        self._restored_data.pop("last_downloaded_map_id", None)
        self._restored_data.pop("last_downloaded_map_version", None)
        await self.store.async_save({})
        await self.async_request_refresh()

    def _delete_cached_map_files(self, archive: Path) -> None:
        try:
            if archive.exists():
                archive.unlink()
        except FileNotFoundError:
            pass
        try:
            if self.debug_dir.exists():
                for child in self.debug_dir.iterdir():
                    if child.is_file():
                        child.unlink()
        except FileNotFoundError:
            pass

    def _write_binary_file(self, archive: Path, content: bytes) -> None:
        archive.parent.mkdir(parents=True, exist_ok=True)
        archive.write_bytes(content)

    async def async_refresh_map_from_archive(self, archive_path: str | Path, *, show_labels: bool = True, show_coverage: bool = True) -> None:
        archive = Path(archive_path)
        if not archive.exists():
            _LOGGER.warning("Map archive does not exist yet: %s", archive)
            return
        self.map_archive_path = str(archive)
        self.map_png_bytes = await self.hass.async_add_executor_job(render_archive_to_png_bytes, archive, show_labels, show_coverage)
        self.room_info = await self.hass.async_add_executor_job(extract_room_info_from_archive, archive)
        self.rooms = room_names_from_info(self.room_info)
        await self._refresh_render_metadata(archive)
        if self.rooms and self.selected_room not in self.rooms:
            self.selected_room = self.rooms[0]
        self.last_map_refresh = datetime.now(tz=UTC).isoformat()
        self.async_update_listeners()

    async def async_refresh_map_from_url(self, url: str) -> None:
        self.s3_map_url = url
        await self.async_download_and_render_map(url)

    def _find_routine_commanddef(self, *, name_loc_key: str | None = None, routine_type: str | None = None, friendly_name: str | None = None) -> dict[str, Any] | None:
        routines = (self.data or {}).get("routines") or self._restored_data.get("routines") or []
        for routine in routines:
            if not isinstance(routine, dict):
                continue
            if name_loc_key and routine.get("name_loc_key") == name_loc_key:
                defs = routine.get("commanddefs") or []
                if defs and isinstance(defs[0], dict):
                    return defs[0]
            if friendly_name and str(routine.get("name") or "").strip().lower() == friendly_name.strip().lower():
                defs = routine.get("commanddefs") or []
                if defs and isinstance(defs[0], dict):
                    return defs[0]
            defs = routine.get("commanddefs") or []
            for commanddef in defs:
                if not isinstance(commanddef, dict):
                    continue
                params = commanddef.get("params") or {}
                if routine_type and str(params.get("routine_type") or "").upper() == routine_type.upper():
                    return commanddef
        return None

    def _resolve_selected_room_entry(self) -> dict[str, Any] | None:
        selected = self.selected_room
        if not selected:
            return None
        for room in self.room_info:
            if not isinstance(room, dict):
                continue
            if str(room.get("name") or "") == selected:
                return room
        return None

    def _room_region_candidates(self, room: dict[str, Any] | None) -> list[str]:
        if not isinstance(room, dict):
            return []
        props = room.get("properties") or {}
        room_name = str(props.get("name") or room.get("name") or "").strip()
        name_matched_room_ids: list[str] = []
        active_map = (self.data or self._restored_data or {}).get("active_map") or {}
        rooms_metadata = active_map.get("rooms_metadata") if isinstance(active_map, dict) else None
        if isinstance(rooms_metadata, list) and room_name:
            for item in rooms_metadata:
                if not isinstance(item, dict):
                    continue
                meta = item.get("room_metadata") if isinstance(item.get("room_metadata"), dict) else {}
                meta_name = str(meta.get("name") or "").strip()
                room_id = item.get("room_id")
                if room_id is None:
                    continue
                if meta_name and meta_name.casefold() == room_name.casefold():
                    sval = str(room_id)
                    if sval not in name_matched_room_ids:
                        name_matched_room_ids.append(sval)

        candidates = [
            *name_matched_room_ids,
            room.get("control_room_id"),
            room.get("room_id"),
            room.get("id"),
            room.get("feature_id"),
            room.get("featureId"),
            room.get("region_id"),
            room.get("regionId"),
            room.get("source_region_id"),
            room.get("sourceRegionId"),
            props.get("control_room_id"),
            props.get("region_id"),
            props.get("regionId"),
            props.get("segment_id"),
            props.get("segmentId"),
            props.get("room_id"),
            props.get("roomId"),
            props.get("id"),
            props.get("feature_id"),
            props.get("featureId"),
            props.get("feature_id_str"),
            props.get("featureIdStr"),
            props.get("source_region_id"),
            props.get("sourceRegionId"),
            props.get("feature"),
        ]
        out: list[str] = []
        for value in candidates:
            if value is None:
                continue
            sval = str(value)
            if sval not in out:
                out.append(sval)
        return out

    def _room_feature_context(self, room: dict[str, Any], region_candidates: list[str]) -> dict[str, Any]:
        props = room.get("properties") or {}
        feature_id = room.get("id") or props.get("feature_id") or props.get("featureId") or props.get("id")
        feature_type = props.get("type") or props.get("feature_type") or props.get("featureType") or "room"
        room_name = props.get("name") or room.get("name") or self.selected_room
        return {
            "feature_id": str(feature_id) if feature_id is not None else None,
            "feature_type": str(feature_type) if feature_type is not None else "room",
            "room_name": str(room_name) if room_name is not None else None,
            "region_candidates": region_candidates,
            "raw_room_id": room.get("id"),
            "properties": props,
        }



    def _room_entry_matches_selection_id(self, room: dict[str, Any], selection_id: str) -> bool:
        if not isinstance(room, dict):
            return False
        props = room.get("properties") or {}
        candidates = [
            room.get("id"),
            room.get("room_id"),
            room.get("feature_id"),
            room.get("featureId"),
            room.get("region_id"),
            room.get("regionId"),
            room.get("control_room_id"),
            props.get("id"),
            props.get("room_id"),
            props.get("roomId"),
            props.get("feature_id"),
            props.get("featureId"),
            props.get("region_id"),
            props.get("regionId"),
            props.get("control_room_id"),
            props.get("segment_id"),
            props.get("segmentId"),
        ]
        return any(str(value) == selection_id for value in candidates if value is not None)

    def _resolve_room_entry_by_selection_id(self, selection_id: str) -> dict[str, Any] | None:
        for room in self.room_info:
            if self._room_entry_matches_selection_id(room, selection_id):
                return room
        return None

    async def async_clean_rooms_by_selection_ids(self, selection_ids: list[Any]) -> dict[str, Any]:
        normalized_ids: list[str] = []
        for value in selection_ids:
            if value is None:
                continue
            sval = str(value).strip()
            if sval and sval not in normalized_ids:
                normalized_ids.append(sval)
        if not normalized_ids:
            raise CloudApiError("No room ids supplied")

        robot_state = self.data.get("robot") if isinstance(self.data, dict) else None
        p2map_id = (self.data.get("active_map_id") if isinstance(self.data, dict) else None) or ((robot_state or {}).get("p2map_id") if isinstance(robot_state, dict) else None)
        pmapv_id = (self.data.get("active_map_version") if isinstance(self.data, dict) else None) or ((((robot_state or {}).get("user_p2mapv_id") or (robot_state or {}).get("pmapv_id")) if isinstance(robot_state, dict) else None))

        regions: list[dict[str, Any]] = []
        resolved: list[dict[str, Any]] = []
        for selection_id in normalized_ids:
            room = self._resolve_room_entry_by_selection_id(selection_id)
            if not room:
                resolved.append({"selection_id": selection_id, "matched": False})
                continue
            region_candidates = self._room_region_candidates(room)
            if not region_candidates:
                resolved.append({"selection_id": selection_id, "matched": True, "room_name": room.get("name"), "region_candidates": []})
                continue
            region_id = str(region_candidates[0])
            region_params = self._build_region_cleaning_params(region_id)
            region = {"region_id": region_id, "type": "rid", "params": region_params}
            regions.append(region)
            resolved.append({
                "selection_id": selection_id,
                "matched": True,
                "room_name": str((room.get("properties") or {}).get("name") or room.get("name") or ""),
                "region_candidates": region_candidates,
                "resolved_region_id": region_id,
            })

        if not regions:
            raise CloudApiError(f"No cleanable rooms resolved from ids: {normalized_ids}")

        commanddef: dict[str, Any] = {
            "robot_id": self.robot_blid,
            "command": "start",
            "ordered": 1,
            "params": self._build_top_level_cleaning_params(),
            "regions": regions,
            "_preferred_payload_variant": "robot_command_envelope",
        }
        if p2map_id:
            commanddef["p2map_id"] = p2map_id
        if pmapv_id:
            commanddef["user_p2mapv_id"] = pmapv_id

        await self._write_debug_json("send_command_app_segment_clean_context.json", {
            "selection_ids": normalized_ids,
            "resolved": resolved,
            "commanddef": commanddef,
        })
        result = await self.api.publish_commanddef_via_cloud_mqtt(commanddef)
        await self._write_debug_json("send_command_app_segment_clean_result.json", result)
        if self.data is not None:
            self.data["vacuum_state"] = "cleaning"
        self.async_update_listeners()
        return result

    async def async_select_room(self, option: str) -> None:
        if option not in self.rooms:
            raise CloudApiError(f"Unknown room selection: {option}")
        self.selected_room = option
        if self._restored_data is None:
            self._restored_data = {}
        self._restored_data["selected_room"] = option
        await self._write_debug_json("selected_room.json", {"selected_room": option, "rooms": self.rooms})
        self.async_update_listeners()

    async def async_clean_room_by_name(self, room_name: str) -> dict[str, Any]:
        if room_name not in self.rooms:
            raise CloudApiError(f"Unknown room selection: {room_name}")
        await self.async_select_room(room_name)
        return await self.async_clean_selected_room()

    def _normalize_suction_level_value(self, option: str | None, fallback: int | None = None) -> int | None:
        text = str(option or "").strip()
        if text:
            digits = "".join(ch for ch in text if ch.isdigit())
            if digits:
                try:
                    value = int(digits)
                except ValueError:
                    value = None
                else:
                    if value > 0:
                        return value
        return fallback

    def _normalize_water_level_value(self, option: str | None, fallback: int | None = None) -> int | None:
        text = str(option or "").strip()
        if text:
            digits = "".join(ch for ch in text if ch.isdigit())
            if digits:
                try:
                    value = int(digits)
                except ValueError:
                    value = None
                else:
                    if value > 0:
                        return value
        return fallback

    def _preferred_operating_mode_value(self) -> int | None:
        preferred = str(self.preferred_cleaning_mode() or "").strip().lower()
        if preferred == "mop":
            return 1
        if preferred in {"vacuum + mop", "vacuum+mop", "vacuum_and_mop", "vacuum then mop", "vacuum_then_mop"}:
            return 6 if self.supports_mopping() else 2
        if preferred == "vacuum":
            return 2
        return self.current_operating_mode_value() or (2 if not self.supports_mopping() else None)

    def _profile_name_for_command(self) -> str:
        clean_score = (self.data or self._restored_data or {}).get("clean_score") or {}
        clean_scores = clean_score.get("clean_scores") if isinstance(clean_score, dict) else None
        if isinstance(clean_scores, list):
            for item in clean_scores:
                if not isinstance(item, dict):
                    continue
                profile = item.get("profile")
                if isinstance(profile, str) and profile.strip():
                    return profile.strip()
        return "normal"

    def _default_region_params_from_clean_score(self, region_id: str | None) -> dict[str, Any]:
        if not region_id:
            return {}
        clean_score = (self.data or self._restored_data or {}).get("clean_score") or {}
        clean_scores = clean_score.get("clean_scores") if isinstance(clean_score, dict) else None
        if not isinstance(clean_scores, list):
            return {}
        for item in clean_scores:
            if not isinstance(item, dict):
                continue
            for region in item.get("regions") or []:
                if not isinstance(region, dict):
                    continue
                if str(region.get("region_id") or "") != str(region_id):
                    continue
                prefs = region.get("smart_clean_prefs")
                if isinstance(prefs, dict):
                    return json.loads(json.dumps(prefs))
        return {}

    def _build_region_cleaning_params(self, region_id: str | None = None) -> dict[str, Any]:
        params = self._default_region_params_from_clean_score(region_id)
        if "twoPass" not in params:
            params["twoPass"] = False

        operating_mode = self._preferred_operating_mode_value()
        if operating_mode is not None:
            params["operatingMode"] = operating_mode

        suction_level = self._normalize_suction_level_value(
            self.preferred_suction_level(),
            fallback=(int(params.get("suctionLevel")) if params.get("suctionLevel") is not None else None),
        )
        if suction_level is not None:
            params["suctionLevel"] = suction_level

        water_level = self._normalize_water_level_value(
            self.preferred_water_level(),
            fallback=(int((params.get("padWetness") or {}).get("padPlate")) if isinstance(params.get("padWetness"), dict) and (params.get("padWetness") or {}).get("padPlate") is not None else None),
        )
        if self.supports_mopping() and operating_mode in {1, 3, 6} and water_level is not None:
            params["padWetness"] = {"padPlate": water_level}
        elif operating_mode == 2:
            params.pop("padWetness", None)

        return params

    def _build_top_level_cleaning_params(self) -> dict[str, Any]:
        return {"profile": self._profile_name_for_command()}

    def _build_room_regions(self, room: dict[str, Any], region_candidates: list[str], schema_name: str | None) -> list[dict[str, Any]]:
        schema = schema_name or "feature_id_region_type"
        ctx = self._room_feature_context(room, region_candidates)
        rid = str(region_candidates[0])
        fid = ctx.get("feature_id") or rid
        feature_type = str(ctx.get("feature_type") or "room")
        robot_state = self.data.get("robot") if isinstance(self.data, dict) else None
        p2map_id = None
        pmapv_id = None
        if isinstance(self.data, dict):
            p2map_id = self.data.get("active_map_id")
            pmapv_id = self.data.get("active_map_version")
        if not p2map_id and isinstance(robot_state, dict):
            p2map_id = robot_state.get("p2map_id")
        if not pmapv_id and isinstance(robot_state, dict):
            pmapv_id = robot_state.get("user_p2mapv_id") or robot_state.get("pmapv_id")
        if schema == "feature_id_region_type":
            return [{"region_id": fid, "id": fid, "type": feature_type, "region_type": feature_type}]
        if schema == "feature_id_map_ids":
            region = {"region_id": fid, "id": fid, "type": feature_type, "region_type": feature_type}
            if p2map_id:
                region["p2map_id"] = p2map_id
            if pmapv_id:
                region["user_p2mapv_id"] = pmapv_id
            return [region]
        if schema == "feature_id_only":
            return [{"id": fid, "feature_id": fid, "region_type": feature_type}]
        if schema == "feature_id_plus_region_candidate":
            return [{"id": fid, "feature_id": fid, "region_id": rid, "type": feature_type, "region_type": feature_type}]
        if schema == "feature_id_full":
            region = {"id": fid, "feature_id": fid, "region_id": rid, "type": feature_type, "region_type": feature_type, "name": ctx.get("room_name")}
            if p2map_id:
                region["p2map_id"] = p2map_id
            if pmapv_id:
                region["user_p2mapv_id"] = pmapv_id
            return [region]
        if schema == "id_only":
            return [{"id": rid}]
        if schema == "region_id_only":
            return [{"region_id": rid}]
        if schema == "regionId_only":
            return [{"regionId": rid}]
        if schema == "pmap_region_id":
            region = {"region_id": rid}
            if p2map_id:
                region["p2map_id"] = p2map_id
            if pmapv_id:
                region["user_p2mapv_id"] = pmapv_id
            return [region]
        if schema == "region_room_plus_id":
            return [{"id": rid, "region_id": rid, "type": "rid", "region_type": "room"}]
        if schema == "region_room_plus_map":
            region = {"region_id": rid, "type": "rid", "region_type": "room"}
            if p2map_id:
                region["p2map_id"] = p2map_id
            if pmapv_id:
                region["user_p2mapv_id"] = pmapv_id
            return [region]
        if schema == "region_room_full_refine":
            region = {"id": rid, "region_id": rid, "type": "rid", "region_type": "room"}
            if p2map_id:
                region["p2map_id"] = p2map_id
            if pmapv_id:
                region["user_p2mapv_id"] = pmapv_id
            return [region]
        if schema == "mapbind_root_pmap":
            return [{"region_id": rid, "type": "room", "region_type": "room"}]
        if schema == "mapbind_root_p2map":
            return [{"region_id": rid, "type": "room", "region_type": "room"}]
        if schema == "mapbind_root_both":
            return [{"region_id": rid, "type": "room", "region_type": "room"}]
        geometry = ((room.get("properties") or {}).get("simplifiedGeometry") if isinstance(room, dict) else None)
        if schema == "mapbind_region_with_id":
            return [{"id": rid, "region_id": rid, "type": "room", "region_type": "room"}]
        if schema == "geometry_full":
            region = {"id": rid, "region_id": rid, "type": "room", "region_type": "room", "name": ctx.get("room_name")}
            if isinstance(geometry, dict):
                region["geometry"] = geometry
                region["simplifiedGeometry"] = geometry
            return [region]
        if schema == "geometry_only":
            region = {"region_id": rid, "type": "room", "region_type": "room"}
            if isinstance(geometry, dict):
                region["geometry"] = geometry
            return [region]
        if schema == "simplified_geometry":
            region = {"region_id": rid, "type": "room", "region_type": "room"}
            if isinstance(geometry, dict):
                region["simplifiedGeometry"] = geometry
            return [region]
        if schema == "coordinates_only":
            region = {"region_id": rid, "type": "room", "region_type": "room"}
            if isinstance(geometry, dict):
                region["geometry_type"] = geometry.get("type")
                region["coordinates"] = geometry.get("coordinates")
            return [region]
        return [{"region_id": rid, "type": "rid", "region_type": "room"}]

    async def async_dump_room_debug(self) -> dict[str, Any]:
        room = self._resolve_selected_room_entry()
        region_candidates = self._room_region_candidates(room) if room else []
        robot_state = self.data.get("robot") if isinstance(self.data, dict) else None
        payload = {
            "selected_room": self.selected_room,
            "rooms": self.rooms,
            "room_info": self.room_info,
            "map_render_metadata": self.map_render_metadata,
            "selected_room_entry": room,
            "region_candidates": region_candidates,
            "feature_context": self._room_feature_context(room, region_candidates) if room else None,
            "active_map_id": (self.data.get("active_map_id") if isinstance(self.data, dict) else None),
            "active_map_version": (self.data.get("active_map_version") if isinstance(self.data, dict) else None),
            "robot_map_ids": robot_state if isinstance(robot_state, dict) else None,
            "cloud_transport_debug": self.api.get_cloud_transport_debug_info(self.robot_blid),
        }
        await self._write_debug_json("room_debug_dump.json", payload)
        return payload

    async def async_clean_selected_room_schema(self, schema_name: str) -> dict[str, Any]:
        return await self.async_clean_selected_room_variant(None, schema_name)

    async def async_clean_selected_room(self) -> dict[str, Any]:
        return await self.async_clean_selected_room_variant(None, None)

    async def async_clean_selected_room_variant(self, variant_name: str | None, schema_name: str | None = None) -> dict[str, Any]:
        transport_debug = self.api.get_cloud_transport_debug_info(self.robot_blid)
        await self._write_debug_json("cloud_transport_debug.json", transport_debug)

        room = self._resolve_selected_room_entry()
        if not room:
            raise CloudApiError("No selected room available")

        region_candidates = self._room_region_candidates(room)
        if not region_candidates:
            raise CloudApiError(f"No region id candidates found for room {self.selected_room}")

        robot_state = self.data.get("robot") if isinstance(self.data, dict) else None
        p2map_id = (self.data.get("active_map_id") if isinstance(self.data, dict) else None) or ((robot_state or {}).get("p2map_id") if isinstance(robot_state, dict) else None)
        pmapv_id = (self.data.get("active_map_version") if isinstance(self.data, dict) else None) or ((((robot_state or {}).get("user_p2mapv_id") or (robot_state or {}).get("pmapv_id")) if isinstance(robot_state, dict) else None))
        feature_ctx = self._room_feature_context(room, region_candidates)
        region_id = str(region_candidates[0])
        region_params = self._build_region_cleaning_params(region_id)

        commanddef: dict[str, Any] = {
            "robot_id": self.robot_blid,
            "command": "start",
            "ordered": 1,
            "params": self._build_top_level_cleaning_params(),
            "regions": [
                {
                    "region_id": region_id,
                    "type": "rid",
                    "params": region_params,
                }
            ],
            "_preferred_payload_variant": "robot_command_envelope",
        }
        if p2map_id:
            commanddef["p2map_id"] = p2map_id
        if pmapv_id:
            commanddef["user_p2mapv_id"] = pmapv_id
        if variant_name:
            commanddef["_room_single_variant"] = variant_name
        if schema_name:
            commanddef["_room_region_schema"] = schema_name

        await self._write_debug_json("routine_execute_clean_selected_room_context.json", {
            "selected_room": self.selected_room,
            "room": room,
            "region_candidates": region_candidates,
            "variant_name": variant_name,
            "schema_name": schema_name,
            "feature_context": feature_ctx,
            "robot_state_map_ids": {
                "p2map_id": (self.data.get("active_map_id") if isinstance(self.data, dict) else None),
                "user_p2mapv_id": (self.data.get("active_map_version") if isinstance(self.data, dict) else None),
                "robot_p2map_id": ((self.data.get("robot") or {}).get("p2map_id") if isinstance(self.data, dict) and isinstance(self.data.get("robot"), dict) else None),
                "robot_user_p2mapv_id": ((((self.data.get("robot") or {}).get("user_p2mapv_id")) or ((self.data.get("robot") or {}).get("pmapv_id"))) if isinstance(self.data, dict) and isinstance(self.data.get("robot"), dict) else None),
            },
            "commanddef": commanddef,
        })
        result = await self.api.publish_commanddef_via_cloud_mqtt(commanddef)
        await self._write_debug_json("routine_execute_clean_selected_room_result.json", result)
        if self.data is not None:
            self.data["vacuum_state"] = "cleaning"
        self.async_update_listeners()
        return result

    def _build_clean_all_commanddef(self) -> dict[str, Any]:
        robot_state = self.data.get("robot") if isinstance(self.data, dict) else None
        p2map_id = (self.data.get("active_map_id") if isinstance(self.data, dict) else None) or ((robot_state or {}).get("p2map_id") if isinstance(robot_state, dict) else None)
        pmapv_id = (self.data.get("active_map_version") if isinstance(self.data, dict) else None) or ((((robot_state or {}).get("user_p2mapv_id") or (robot_state or {}).get("pmapv_id")) if isinstance(robot_state, dict) else None))
        params = self._build_top_level_cleaning_params()
        operating_mode = self._preferred_operating_mode_value()
        if operating_mode is not None:
            params["operatingMode"] = operating_mode
        suction_level = self._normalize_suction_level_value(self.preferred_suction_level())
        if suction_level is not None:
            params["suctionLevel"] = suction_level
        water_level = self._normalize_water_level_value(self.preferred_water_level())
        if self.supports_mopping() and operating_mode in {1, 3, 6} and water_level is not None:
            params["padWetness"] = {"padPlate": water_level}
        params["routine_type"] = "CLEAN_ALL"
        commanddef: dict[str, Any] = {
            "robot_id": self.robot_blid,
            "command": "start",
            "params": params,
            "_preferred_payload_variant": "commanddef_wrapped",
        }
        if p2map_id:
            commanddef["p2map_id"] = p2map_id
        if pmapv_id:
            commanddef["user_p2mapv_id"] = pmapv_id
        return commanddef

    async def async_start_clean_all(self) -> dict[str, Any]:
        result = await self.async_execute_named_routine("clean_all")
        await self._apply_optimistic_state("cleaning", "run", "Roomba is cleaning", command_name="clean_all", preserve_seconds=20)
        return result

    async def async_pause_cleaning(self) -> dict[str, Any]:
        result = await self.api.async_send_simple_command(self.robot_blid, "pause")
        await self._apply_optimistic_state("paused", "pause", "Roomba is paused", command_name="pause", preserve_seconds=5)
        return result

    async def async_stop_cleaning(self) -> dict[str, Any]:
        result = await self.api.async_send_simple_command(self.robot_blid, "stop")
        await self._apply_optimistic_state("idle", "stop", "Roomba is idle", command_name="stop", preserve_seconds=5)
        return result

    async def async_resume_cleaning(self) -> dict[str, Any]:
        result = await self.api.async_send_simple_command(self.robot_blid, "resume")
        await self._apply_optimistic_state("cleaning", "resume", "Roomba is cleaning", command_name="resume", preserve_seconds=5)
        return result

    async def async_return_to_base(self) -> dict[str, Any]:
        result = await self.api.async_send_simple_command(self.robot_blid, "dock")
        await self._apply_optimistic_state("returning", "returning", "Roomba is returning to the dock", command_name="dock", preserve_seconds=90)
        return result

    async def async_execute_named_routine(self, routine_key: str) -> dict[str, Any]:
        transport_debug = self.api.get_cloud_transport_debug_info(self.robot_blid)
        await self._write_debug_json("cloud_transport_debug.json", transport_debug)

        if routine_key == "clean_all":
            commanddef = self._build_clean_all_commanddef()
            debug_prefix = "clean_all"
        elif routine_key == "spot_clean":
            commanddef = self._find_routine_commanddef(name_loc_key="digital_spot_clean", routine_type="SPOT_CLEAN", friendly_name="Spot Clean")
            debug_prefix = "spot_clean"
        else:
            raise CloudApiError(f"Unknown routine key: {routine_key}")

        if not commanddef:
            raise CloudApiError(f"No command definition found for routine {routine_key}")

        await self._write_debug_json(f"routine_execute_{debug_prefix}_commanddef.json", commanddef)
        result = await self.api.publish_commanddef_via_cloud_mqtt(commanddef)
        await self._write_debug_json(f"routine_execute_{debug_prefix}_result.json", result)
        return result
