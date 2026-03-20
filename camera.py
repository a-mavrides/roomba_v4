
from __future__ import annotations

import io
from math import cos, degrees, radians, sin
from typing import Any

from PIL import Image, ImageDraw

from homeassistant.components.camera import Camera
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import RoombaV4ConfigEntry
from .const import DOMAIN
from .entity import RoombaV4Entity


# Static robot/map alignment trim in meters.
# Applied only at the final display/attribute stage so internal path logic
# continues to use the original stable coordinate space.
DISPLAY_OFFSET_X = -0.03
DISPLAY_OFFSET_Y = 0.02


def _apply_display_offset(point: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(point, dict):
        return point
    x = point.get("x")
    y = point.get("y")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return dict(point)
    shifted = dict(point)
    shifted["x"] = float(x) + DISPLAY_OFFSET_X
    shifted["y"] = float(y) + DISPLAY_OFFSET_Y
    return shifted


def _apply_display_offset_to_points(points: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    shifted: list[dict[str, Any]] = []
    for pt in points or []:
        if not isinstance(pt, dict):
            continue
        shifted_pt = _apply_display_offset(pt)
        if isinstance(shifted_pt, dict):
            shifted.append(shifted_pt)
    return shifted




def _vacuum_to_image_xy(x: float | int | None, y: float | int | None, meta: dict[str, Any]) -> tuple[float, float] | None:
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return None
    bounds = meta.get("render_bounds") if isinstance(meta.get("render_bounds"), dict) else (meta.get("bounds") if isinstance(meta.get("bounds"), dict) else None)
    image = meta.get("image") if isinstance(meta.get("image"), dict) else None
    if not bounds or not image:
        return None
    try:
        min_x = float(bounds.get("min_x"))
        max_x = float(bounds.get("max_x"))
        min_y = float(bounds.get("min_y"))
        max_y = float(bounds.get("max_y"))
        width = float(image.get("width"))
        height = float(image.get("height"))
    except (TypeError, ValueError):
        return None
    dx = max(max_x - min_x, 1e-9)
    dy = max(max_y - min_y, 1e-9)
    px = (float(x) - min_x) / dx * width
    py = height - ((float(y) - min_y) / dy * height)
    return (px, py)


def _draw_overlay_png(base_png: bytes | None, meta: dict[str, Any], vacuum_position: dict[str, Any] | None, path_points: list[dict[str, Any]], return_path_points: list[dict[str, Any]] | None = None) -> bytes | None:
    if not base_png:
        return base_png
    try:
        img = Image.open(io.BytesIO(base_png)).convert("RGBA")
    except Exception:
        return base_png

    draw = ImageDraw.Draw(img, "RGBA")

    # cleaned path / recent route
    poly = []
    for pt in path_points or []:
        if not isinstance(pt, dict):
            continue
        pix = _vacuum_to_image_xy(pt.get("x"), pt.get("y"), meta)
        if pix:
            poly.append(pix)
    if len(poly) >= 2:
        draw.line(poly, fill=(0, 120, 255, 235), width=7)
        for px, py in poly[-400:]:
            draw.ellipse((px-3, py-3, px+3, py+3), fill=(0, 120, 255, 215))

    # return-to-dock guide / predicted route
    return_poly = []
    for pt in return_path_points or []:
        if not isinstance(pt, dict):
            continue
        pix = _vacuum_to_image_xy(pt.get("x"), pt.get("y"), meta)
        if pix:
            return_poly.append(pix)
    if len(return_poly) >= 2:
        draw.line(return_poly, fill=(249, 115, 22, 220), width=5)

    # robot body + heading
    if isinstance(vacuum_position, dict):
        pix = _vacuum_to_image_xy(vacuum_position.get("x"), vacuum_position.get("y"), meta)
        if pix:
            cx, cy = pix
            r = 10
            draw.ellipse((cx-r, cy-r, cx+r, cy+r), fill=(220, 38, 38, 235), outline=(255,255,255,255), width=2)
            angle = vacuum_position.get("angle")
            if isinstance(angle, (int, float)):
                # 0 deg = east in map coords; invert Y for image coords
                a = radians(float(angle))
                tip = (cx + cos(a) * 18, cy - sin(a) * 18)
                left = (cx + cos(a + 2.45) * 9, cy - sin(a + 2.45) * 9)
                right = (cx + cos(a - 2.45) * 9, cy - sin(a - 2.45) * 9)
                draw.polygon([tip, left, right], fill=(255,255,255,240))

    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def _distance_between_points(a: dict[str, Any] | None, b: dict[str, Any] | None) -> float | None:
    if not isinstance(a, dict) or not isinstance(b, dict):
        return None
    ax = a.get("x")
    ay = a.get("y")
    bx = b.get("x")
    by = b.get("y")
    if not all(isinstance(v, (int, float)) for v in (ax, ay, bx, by)):
        return None
    dx = float(ax) - float(bx)
    dy = float(ay) - float(by)
    return (dx * dx + dy * dy) ** 0.5


def _vacuum_state_lower(data: dict[str, Any] | None, status: dict[str, Any] | None = None) -> str:
    if not isinstance(data, dict):
        data = {}
    if not isinstance(status, dict):
        status = data.get("status") if isinstance(data.get("status"), dict) else {}
    return str(data.get("vacuum_state") or status.get("vacuum_state") or "").lower()


def _mission_phase_lower(status: dict[str, Any] | None) -> str:
    if not isinstance(status, dict):
        return ""
    return str(status.get("mission_phase") or "").lower()


def _is_returning_phase(phase: str) -> bool:
    return phase in {"hmmidmssn", "return", "returning", "homing", "dockroute", "dockroutecomplete", "hmusrdock"}


def _is_returning_state(vacuum_state: str, phase: str) -> bool:
    vacuum_state = str(vacuum_state or "").lower()
    if vacuum_state in {"returning", "returning_to_dock", "returning to dock", "homing", "docking"}:
        return True
    return _is_returning_phase(phase)


def _raw_live_vacuum_position(data_or_status: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(data_or_status, dict):
        return None

    # Prefer truly raw live-state coordinates during return-to-dock.
    live_state = data_or_status.get("live_state") if isinstance(data_or_status.get("live_state"), dict) else {}
    livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
    current = livemap.get("current") if isinstance(livemap.get("current"), dict) else {}
    if isinstance(current.get("x"), (int, float)) and isinstance(current.get("y"), (int, float)):
        theta = current.get("theta")
        angle = _normalize_angle_degrees(theta)
        return {
            "x": float(current.get("x")),
            "y": float(current.get("y")),
            "a": angle,
            "angle": angle,
            "theta": theta,
            "source": "livemap_current",
        }

    pose = live_state.get("pose") if isinstance(live_state.get("pose"), dict) else {}
    point = pose.get("point") if isinstance(pose.get("point"), dict) else {}
    if isinstance(point.get("x"), (int, float)) and isinstance(point.get("y"), (int, float)):
        theta = pose.get("theta")
        angle = _normalize_angle_degrees(theta)
        return {
            "x": float(point.get("x")),
            "y": float(point.get("y")),
            "a": angle,
            "angle": angle,
            "theta": theta,
            "source": "pose",
        }

    status = data_or_status.get("status") if isinstance(data_or_status.get("status"), dict) else data_or_status
    x = status.get("x")
    y = status.get("y")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return None
    theta = status.get("theta")
    angle = _normalize_angle_degrees(theta)
    return {
        "x": float(x),
        "y": float(y),
        "a": angle,
        "angle": angle,
        "theta": theta,
        "source": str(status.get("pose_source") or "status"),
    }


def _is_paused_state(vacuum_state: str, phase: str) -> bool:
    return vacuum_state in {"paused"} or phase in {"pause", "paused"}


def _is_docked_state(vacuum_state: str, phase: str, dock_contact: bool) -> bool:
    return dock_contact or vacuum_state in {"docked", "charging"} or phase in {"charge", "chargecompleted", "dockend", "dock"}


def _filter_path_points(path_points: list[dict[str, Any]] | None, vacuum_position: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for pt in path_points or []:
        if not isinstance(pt, dict):
            continue
        x = pt.get("x")
        y = pt.get("y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            continue
        candidate = {
            "x": float(x),
            "y": float(y),
            **({"theta": pt.get("theta")} if pt.get("theta") is not None else {}),
            **({"flag": pt.get("flag")} if pt.get("flag") is not None else {}),
        }
        if filtered:
            last = filtered[-1]
            distance = _distance_between_points(last, candidate)
            if distance is not None and distance < 0.01:
                continue
            if len(filtered) >= 2:
                prev = filtered[-2]
                prev_distance = _distance_between_points(prev, candidate)
                if (
                    distance is not None
                    and prev_distance is not None
                    and distance > 0.35
                    and prev_distance < 0.05
                ):
                    filtered[-1] = candidate
                    continue
        filtered.append(candidate)

    if vacuum_position and filtered:
        last = filtered[-1]
        distance = _distance_between_points(last, vacuum_position)
        if distance is not None and 0.01 < distance < 0.6:
            filtered.append({
                "x": float(vacuum_position.get("x")),
                "y": float(vacuum_position.get("y")),
                **({"theta": vacuum_position.get("theta")} if vacuum_position.get("theta") is not None else {}),
            })
    return filtered[-2500:]


def _room_center_from_bounds(x0: float, y0: float, x1: float, y1: float) -> tuple[float, float]:
    return ((float(x0) + float(x1)) / 2.0, (float(y0) + float(y1)) / 2.0)


def _extract_room_box(room: dict[str, Any]) -> dict[str, float] | None:
    props = room.get("properties") or {}
    candidates = [room, props]
    for source in candidates:
        if not isinstance(source, dict):
            continue
        if all(source.get(k) is not None for k in ("x0", "y0", "x1", "y1")):
            try:
                return {
                    "x0": float(source.get("x0")),
                    "y0": float(source.get("y0")),
                    "x1": float(source.get("x1")),
                    "y1": float(source.get("y1")),
                }
            except (TypeError, ValueError):
                pass
        bbox = source.get("bbox") or source.get("bounds")
        if isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
            try:
                return {
                    "x0": float(bbox[0]),
                    "y0": float(bbox[1]),
                    "x1": float(bbox[2]),
                    "y1": float(bbox[3]),
                }
            except (TypeError, ValueError):
                pass
        simplified = source.get("simplifiedGeometry") or source.get("geometry")
        if isinstance(simplified, dict):
            coords = simplified.get("coordinates")
            points: list[tuple[float, float]] = []
            def walk(v: Any):
                if isinstance(v, (list, tuple)):
                    if len(v) >= 2 and isinstance(v[0], (int, float)) and isinstance(v[1], (int, float)):
                        points.append((float(v[0]), float(v[1])))
                    else:
                        for item in v:
                            walk(item)
            walk(coords)
            if points:
                xs = [pt[0] for pt in points]
                ys = [pt[1] for pt in points]
                return {"x0": min(xs), "y0": min(ys), "x1": max(xs), "y1": max(ys)}
    return None


def _build_xiaomi_rooms(room_info: list[dict[str, Any]]) -> dict[str, Any]:
    rooms: dict[str, Any] = {}
    for index, room in enumerate(room_info, start=1):
        control_room_id = room.get("control_room_id") or (room.get("properties") or {}).get("control_room_id") or (room.get("properties") or {}).get("room_id") or room.get("room_id")
        room_id = str(control_room_id or room.get("id") or index)
        name = str(room.get("name") or f"Room {index}")
        box = _extract_room_box(room)
        payload: dict[str, Any] = {
            "room_id": room_id,
            "name": name,
            "custom_name": name,
            "cleaning_times": 1,
            "suction_level": 1,
            "water_volume": 1,
            "cleaning_mode": 2,
            "type": 0,
            "index": 0,
            "icon": "mdi:home-outline",
            "color_index": (index - 1) % 4,
            "unique_id": room_id,
            "visibility": "Visible",
        }
        if box:
            scaled_box = {k: _scale_coord(v) for k, v in box.items()}
            payload.update(scaled_box)
            cx, cy = _room_center_from_bounds(box["x0"], box["y0"], box["x1"], box["y1"])
            payload["x"] = _scale_coord(cx)
            payload["y"] = _scale_coord(cy)
        rooms[room_id] = payload
    return rooms

def _scale_coord(value: Any, factor: float = 1000.0) -> int | None:
    if not isinstance(value, (int, float)):
        return None
    return int(round(float(value) * factor))


def _scale_point_dict(point: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(point, dict):
        return None
    x = _scale_coord(point.get("x"))
    y = _scale_coord(point.get("y"))
    if x is None or y is None:
        return None
    out = {"x": x, "y": y}
    if point.get("a") is not None:
        try:
            out["a"] = int(round(float(point.get("a"))))
        except (TypeError, ValueError):
            pass
    if point.get("angle") is not None and "a" not in out:
        try:
            out["a"] = int(round(float(point.get("angle"))))
        except (TypeError, ValueError):
            pass
    return out


def _scale_calibration_points(points: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    scaled = []
    for pt in points or []:
        if not isinstance(pt, dict):
            continue
        mp = pt.get("map") if isinstance(pt.get("map"), dict) else None
        vac = pt.get("vacuum") if isinstance(pt.get("vacuum"), dict) else None
        if not mp or not vac:
            continue
        vx = _scale_coord(vac.get("x"))
        vy = _scale_coord(vac.get("y"))
        if vx is None or vy is None:
            continue
        scaled.append({"map": {"x": mp.get("x"), "y": mp.get("y")}, "vacuum": {"x": vx, "y": vy}})
    return scaled


def _normalize_angle_degrees(theta: float | int | None) -> float | None:
    if not isinstance(theta, (int, float)):
        return None
    angle = float(degrees(theta))
    while angle <= -180:
        angle += 360
    while angle > 180:
        angle -= 360
    return angle




def _effective_vacuum_position(status: dict[str, Any], meta: dict[str, Any], last_active: dict[str, Any] | None = None, vacuum_state: str | None = None) -> dict[str, Any] | None:
    theta = status.get("theta")
    angle = _normalize_angle_degrees(theta)
    vacuum_position = None
    if status.get("x") is not None and status.get("y") is not None:
        vacuum_position = {
            "x": status.get("x"),
            "y": status.get("y"),
            "a": angle,
            "angle": angle,
            "theta": theta,
        }

    phase = str(status.get("mission_phase") or "").lower()
    vacuum_state = str(vacuum_state or status.get("vacuum_state") or "").lower()
    dock_contact = status.get("dock_contact") is True
    charger = meta.get("charger") if isinstance(meta.get("charger"), dict) else None
    charger_position = None
    if charger and isinstance(charger.get("x"), (int, float)) and isinstance(charger.get("y"), (int, float)):
        charger_position = {
            "x": float(charger.get("x")),
            "y": float(charger.get("y")),
            "a": charger.get("angle") if charger.get("angle") is not None else charger.get("a"),
            "angle": charger.get("angle") if charger.get("angle") is not None else charger.get("a"),
            "theta": theta,
        }

    definitely_docked = dock_contact or vacuum_state in {"docked", "charging"} or phase in {"charge", "chargecompleted", "dockend", "dock"}
    returning = _is_returning_state(vacuum_state, phase)

    if definitely_docked and charger_position is not None:
        return charger_position
    if returning and last_active is not None:
        return dict(last_active)
    return vacuum_position

def _build_valetudo_map_data(meta: dict[str, Any], vacuum_position: dict[str, Any] | None, path_points: list[dict[str, Any]]) -> dict[str, Any]:
    entities: list[dict[str, Any]] = []

    charger = meta.get("charger") if isinstance(meta.get("charger"), dict) else None
    if charger and charger.get("x") is not None and charger.get("y") is not None:
        charger_angle = charger.get("angle")
        if charger_angle is None:
            charger_angle = charger.get("a")
        entities.append({
            "type": "charger",
            "points": [{"x": _scale_coord(charger.get("x")), "y": _scale_coord(charger.get("y"))}],
            "metaData": {"angle": charger_angle},
        })

    if vacuum_position and vacuum_position.get("x") is not None and vacuum_position.get("y") is not None:
        entities.append({
            "type": "robot",
            "points": [{"x": _scale_coord(vacuum_position.get("x")), "y": _scale_coord(vacuum_position.get("y"))}],
            "metaData": {"angle": vacuum_position.get("angle")},
        })

    polyline = [
        {"x": _scale_coord(pt.get("x")), "y": _scale_coord(pt.get("y"))}
        for pt in path_points
        if isinstance(pt, dict) and pt.get("x") is not None and pt.get("y") is not None
    ]
    if polyline:
        entities.append({
            "type": "path",
            "points": polyline,
            "metaData": {"point_count": len(polyline)},
        })

    map_data: dict[str, Any] = {
        "calibration_points": _scale_calibration_points(meta.get("calibration_points") or []),
        "entities": entities,
        "image": meta.get("image") or {},
        "layers": [],
    }
    bounds = meta.get("render_bounds") if isinstance(meta.get("render_bounds"), dict) else (meta.get("bounds") if isinstance(meta.get("bounds"), dict) else None)
    if bounds:
        map_data["bounds"] = bounds
    return map_data

async def async_setup_entry(hass: HomeAssistant, entry: RoombaV4ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    camera = RoombaV4MapCamera(coordinator)
    coordinator.map_camera = camera
    async_add_entities([camera])

class RoombaV4MapCamera(RoombaV4Entity, Camera):
    _attr_name = "Map"

    def __init__(self, coordinator) -> None:
        Camera.__init__(self)
        RoombaV4Entity.__init__(self, coordinator, "map")
        self._last_active_vacuum_position: dict[str, Any] | None = None
        self._local_path_points: list[dict[str, Any]] = []
        self._recent_pose_candidates: list[dict[str, Any]] = []
        self._return_path_points: list[dict[str, Any]] = []
        self._recent_return_pose_candidates: list[dict[str, Any]] = []
        self._was_returning: bool = False


    def _clear_local_path(self) -> None:
        self._local_path_points = []
        self._recent_pose_candidates = []

    def _clear_return_path(self) -> None:
        self._return_path_points = []
        self._recent_return_pose_candidates = []

    async def async_clear_path_history(self) -> None:
        self._clear_local_path()
        self._clear_return_path()
        self.async_write_ha_state()

    def _is_effectively_docked(self, status: dict[str, Any], vacuum_position: dict[str, Any] | None, charger_position: dict[str, Any] | None) -> bool:
        vacuum_state = _vacuum_state_lower(self.coordinator.data or {}, status)
        phase = _mission_phase_lower(status)
        if vacuum_state in {"docked", "charging"} or phase in {"charge", "chargecompleted", "dockend", "dock"} or status.get("dock_contact") is True:
            return True
        if isinstance(vacuum_position, dict) and isinstance(charger_position, dict):
            distance = _distance_between_points(vacuum_position, charger_position)
            if distance is not None and distance < 0.03:
                return True
            if distance is not None and distance < 0.12 and vacuum_state in {"idle", "ready", "unknown"} and not _is_returning_state(vacuum_state, phase):
                return True
        return False

    def _update_local_path(self, status: dict[str, Any], vacuum_position: dict[str, Any] | None, charger_position: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(vacuum_position, dict):
            return list(self._local_path_points)

        vacuum_state = _vacuum_state_lower(self.coordinator.data or {}, status)
        phase = _mission_phase_lower(status)

        if self._is_effectively_docked(status, vacuum_position, charger_position):
            self._recent_pose_candidates = []
            return list(self._local_path_points)

        if _is_paused_state(vacuum_state, phase):
            self._recent_pose_candidates = []
            return list(self._local_path_points)

        raw_candidate = {
            "x": float(vacuum_position.get("x")),
            "y": float(vacuum_position.get("y")),
            **({"theta": vacuum_position.get("theta")} if vacuum_position.get("theta") is not None else {}),
            "flag": "camera_local_pose_raw",
        }
        self._recent_pose_candidates.append(raw_candidate)
        self._recent_pose_candidates = self._recent_pose_candidates[-5:]

        smoothing_window = self._recent_pose_candidates[-3:]
        xs = [float(pt["x"]) for pt in smoothing_window if isinstance(pt.get("x"), (int, float))]
        ys = [float(pt["y"]) for pt in smoothing_window if isinstance(pt.get("y"), (int, float))]
        if not xs or not ys:
            return list(self._local_path_points)
        xs.sort()
        ys.sort()
        candidate = {
            "x": xs[len(xs) // 2],
            "y": ys[len(ys) // 2],
            **({"theta": vacuum_position.get("theta")} if vacuum_position.get("theta") is not None else {}),
            "flag": "camera_local_pose",
        }

        if not self._local_path_points and isinstance(charger_position, dict):
            start_distance = _distance_between_points(candidate, charger_position)
            if start_distance is not None and start_distance < 0.22:
                self._local_path_points.append({
                    "x": float(charger_position.get("x")),
                    "y": float(charger_position.get("y")),
                    "flag": "dock_anchor",
                })

        # When a run just starts, keep the trail anchored at the dock and ignore
        # noisy early poses until the robot has clearly moved away from the charger.
        if len(self._local_path_points) == 1 and self._local_path_points[0].get("flag") == "dock_anchor" and isinstance(charger_position, dict):
            start_distance = _distance_between_points(candidate, charger_position)
            recent = self._recent_pose_candidates[-3:]
            cluster_ok = False
            if len(recent) >= 2:
                spread = [_distance_between_points(candidate, pt) for pt in recent if isinstance(pt, dict)]
                valid_spread = [d for d in spread if d is not None]
                cluster_ok = bool(valid_spread) and max(valid_spread) < 0.08
            if start_distance is None or start_distance < 0.14 or not cluster_ok:
                return list(self._local_path_points)

        existing = self._local_path_points[-1] if self._local_path_points else None
        if isinstance(existing, dict):
            distance = _distance_between_points(existing, candidate)
            if distance is not None:
                if distance < 0.05:
                    return list(self._local_path_points)
                if distance > 0.45:
                    return list(self._local_path_points)
                if len(self._local_path_points) >= 2:
                    prev = self._local_path_points[-2]
                    prev_distance = _distance_between_points(prev, candidate)
                    if prev_distance is not None and prev_distance < 0.10 and distance > 0.18:
                        return list(self._local_path_points)

        self._local_path_points.append(candidate)
        self._local_path_points = self._local_path_points[-1200:]
        return list(self._local_path_points)

    def _display_vacuum_position(
        self,
        status: dict[str, Any],
        vacuum_position: dict[str, Any] | None,
        charger_position: dict[str, Any] | None,
        path_points: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, str]:
        if not isinstance(vacuum_position, dict):
            return None, "none"

        vacuum_state = _vacuum_state_lower(self.coordinator.data or {}, status)
        phase = _mission_phase_lower(status)

        if self._is_effectively_docked(status, vacuum_position, charger_position) and isinstance(charger_position, dict):
            display = dict(vacuum_position)
            display["x"] = float(charger_position.get("x"))
            display["y"] = float(charger_position.get("y"))
            if charger_position.get("a") is not None and display.get("angle") is None:
                display["a"] = charger_position.get("a")
                display["angle"] = charger_position.get("a")
            return display, "charger"

        if _is_returning_state(vacuum_state, phase):
            if self._return_path_points:
                display = dict(vacuum_position)
                end = self._return_path_points[-1]
                display["x"] = float(end.get("x"))
                display["y"] = float(end.get("y"))
                return display, "return_path_end"
            if getattr(self, "_last_active_vacuum_position", None):
                return dict(self._last_active_vacuum_position), "return_last_active"

        if _is_paused_state(vacuum_state, phase):
            if path_points:
                display = dict(vacuum_position)
                display["x"] = float(path_points[-1].get("x"))
                display["y"] = float(path_points[-1].get("y"))
                return display, "paused_path_end"
            if getattr(self, "_last_active_vacuum_position", None):
                return dict(self._last_active_vacuum_position), "paused_last_active"

        if path_points:
            path_end = path_points[-1]
            end_distance = _distance_between_points(vacuum_position, path_end)
            if end_distance is not None and end_distance <= 0.35 and not _is_returning_phase(phase):
                display = dict(vacuum_position)
                display["x"] = float(path_end.get("x"))
                display["y"] = float(path_end.get("y"))
                if isinstance(charger_position, dict) and len(path_points) <= 2:
                    dock_distance = _distance_between_points(path_end, charger_position)
                    if dock_distance is not None and dock_distance < 0.22:
                        display["x"] = float(charger_position.get("x"))
                        display["y"] = float(charger_position.get("y"))
                        return display, "path_end_dock_anchor"
                return display, "path_end"

        return dict(vacuum_position), "live"

    def _update_return_path(
        self,
        status: dict[str, Any],
        raw_live_vacuum_position: dict[str, Any] | None,
        charger_position: dict[str, Any] | None,
        path_points: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        vacuum_state = _vacuum_state_lower(self.coordinator.data or {}, status)
        phase = _mission_phase_lower(status)
        returning = _is_returning_state(vacuum_state, phase)

        if self._is_effectively_docked(status, raw_live_vacuum_position, charger_position):
            if self._was_returning:
                self._clear_local_path()
                self._clear_return_path()
            self._was_returning = False
            return []

        if not returning or not isinstance(charger_position, dict):
            if not self._was_returning:
                self._clear_return_path()
            self._was_returning = False
            return list(self._return_path_points) if self._return_path_points else []

        self._was_returning = True
        if not isinstance(raw_live_vacuum_position, dict):
            return list(self._return_path_points)

        raw_candidate = {
            "x": float(raw_live_vacuum_position.get("x")),
            "y": float(raw_live_vacuum_position.get("y")),
            **({"theta": raw_live_vacuum_position.get("theta")} if raw_live_vacuum_position.get("theta") is not None else {}),
            "flag": "return_pose_raw",
        }
        self._recent_return_pose_candidates.append(raw_candidate)
        self._recent_return_pose_candidates = self._recent_return_pose_candidates[-5:]

        smoothing_window = self._recent_return_pose_candidates[-3:]
        xs = [float(pt["x"]) for pt in smoothing_window if isinstance(pt.get("x"), (int, float))]
        ys = [float(pt["y"]) for pt in smoothing_window if isinstance(pt.get("y"), (int, float))]
        if not xs or not ys:
            return list(self._return_path_points)
        xs.sort()
        ys.sort()
        candidate = {
            "x": xs[len(xs) // 2],
            "y": ys[len(ys) // 2],
            **({"theta": raw_live_vacuum_position.get("theta")} if raw_live_vacuum_position.get("theta") is not None else {}),
            "flag": "return_pose",
        }

        if not self._return_path_points:
            start = path_points[-1] if path_points else raw_live_vacuum_position
            if isinstance(start, dict):
                self._return_path_points.append({
                    "x": float(start.get("x")),
                    "y": float(start.get("y")),
                    "flag": "return_start",
                })

        existing = self._return_path_points[-1] if self._return_path_points else None
        if isinstance(existing, dict):
            distance = _distance_between_points(existing, candidate)
            if distance is not None:
                if distance < 0.01:
                    return list(self._return_path_points)
                if distance > 0.60:
                    return list(self._return_path_points)
                if len(self._return_path_points) >= 2:
                    prev = self._return_path_points[-2]
                    prev_distance = _distance_between_points(prev, candidate)
                    if prev_distance is not None and prev_distance < 0.05 and distance > 0.25:
                        return list(self._return_path_points)

            # Reject points that suddenly move away from the dock while already returning.
            existing_dock_distance = _distance_between_points(existing, charger_position)
            candidate_dock_distance = _distance_between_points(candidate, charger_position)
            if (
                existing_dock_distance is not None
                and candidate_dock_distance is not None
                and candidate_dock_distance > existing_dock_distance + 0.20
            ):
                return list(self._return_path_points)

        self._return_path_points.append(candidate)
        self._return_path_points = self._return_path_points[-1200:]

        dock_distance = _distance_between_points(candidate, charger_position)
        if dock_distance is not None and dock_distance < 0.10:
            last = self._return_path_points[-1] if self._return_path_points else None
            final_gap = _distance_between_points(last, charger_position)
            if final_gap is None or final_gap > 0.01:
                self._return_path_points.append({
                    "x": float(charger_position.get("x")),
                    "y": float(charger_position.get("y")),
                    "flag": "return_dock",
                })

        return list(self._return_path_points)

    async def async_camera_image(self, width=None, height=None) -> bytes | None:
        data = self.coordinator.data or {}
        status = data.get("status") or {}
        live_state = data.get("live_state") or {}
        livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
        meta = self.coordinator.map_render_metadata or {}
        raw_live_vacuum_position = _raw_live_vacuum_position(data)
        vacuum_position = _effective_vacuum_position(
            status,
            meta,
            getattr(self, "_last_active_vacuum_position", None),
            str(data.get("vacuum_state") or ""),
        )
        charger = meta.get("charger") if isinstance(meta.get("charger"), dict) else None
        charger_position = None
        if charger and charger.get("x") is not None and charger.get("y") is not None:
            charger_position = {"x": float(charger.get("x")), "y": float(charger.get("y")), "a": charger.get("angle") if charger.get("angle") is not None else charger.get("a")}
        raw_path_points = livemap.get("cumulative_path_points") if isinstance(livemap.get("cumulative_path_points"), list) else (livemap.get("path_points") if isinstance(livemap.get("path_points"), list) else [])
        local_path_points = self._update_local_path(status, vacuum_position, charger_position)
        path_points = _filter_path_points([*(raw_path_points or []), *local_path_points], vacuum_position)
        return_path_points = self._update_return_path(status, raw_live_vacuum_position, charger_position, path_points)
        display_vacuum_position, _display_position_mode = self._display_vacuum_position(status, vacuum_position, charger_position, path_points)
        draw_path_points = _apply_display_offset_to_points(path_points)
        draw_return_path_points = _apply_display_offset_to_points(return_path_points)
        draw_vacuum_position = _apply_display_offset(display_vacuum_position)
        phase = _mission_phase_lower(status)
        vacuum_state = _vacuum_state_lower(data, status)
        if not self._is_effectively_docked(status, raw_live_vacuum_position or vacuum_position, charger_position) and not _is_returning_state(vacuum_state, phase) and not _is_paused_state(vacuum_state, phase):
            if display_vacuum_position is not None:
                self._last_active_vacuum_position = dict(display_vacuum_position)
        return await self.coordinator.hass.async_add_executor_job(
            _draw_overlay_png, self.coordinator.map_png_bytes, meta, draw_vacuum_position, draw_path_points, draw_return_path_points
        )

    @property
    def extra_state_attributes(self):
        data = self.coordinator.data or {}
        status = data.get("status") or {}
        live_state = data.get("live_state") or {}
        livemap = live_state.get("livemap") if isinstance(live_state.get("livemap"), dict) else {}
        meta = self.coordinator.map_render_metadata or {}
        raw_live_vacuum_position = _raw_live_vacuum_position(data)
        vacuum_state = _vacuum_state_lower(data, status)
        phase = _mission_phase_lower(status)
        vacuum_position = _effective_vacuum_position(
            status,
            meta,
            getattr(self, "_last_active_vacuum_position", None),
            str(data.get("vacuum_state") or ""),
        )
        charger = meta.get("charger") if isinstance(meta.get("charger"), dict) else None
        charger_position = None
        if charger and charger.get("x") is not None and charger.get("y") is not None:
            charger_position = {"x": float(charger.get("x")), "y": float(charger.get("y")), "a": charger.get("angle") if charger.get("angle") is not None else charger.get("a")}
        raw_path_points = livemap.get("cumulative_path_points") if isinstance(livemap.get("cumulative_path_points"), list) else (livemap.get("path_points") if isinstance(livemap.get("path_points"), list) else [])
        local_path_points = self._update_local_path(status, vacuum_position, charger_position)
        path_points = _filter_path_points([*(raw_path_points or []), *local_path_points], vacuum_position)
        return_path_points = self._update_return_path(status, raw_live_vacuum_position, charger_position, path_points)
        display_vacuum_position, display_position_mode = self._display_vacuum_position(status, vacuum_position, charger_position, path_points)
        display_path_points = _apply_display_offset_to_points(path_points)
        display_return_path_points = _apply_display_offset_to_points(return_path_points)
        display_vacuum_position = _apply_display_offset(display_vacuum_position)
        display_charger_position = _apply_display_offset(charger_position)
        card_path = None
        if display_path_points:
            path_pairs = [
                [pt.get("x"), pt.get("y")]
                for pt in display_path_points
                if isinstance(pt, dict) and pt.get("x") is not None and pt.get("y") is not None
            ]
            if path_pairs:
                card_path = {
                    "point_length": len(path_pairs),
                    "point_size": 2,
                    "angle": 0,
                    "path": path_pairs,
                }
        map_data = _build_valetudo_map_data(meta, display_vacuum_position, display_path_points)
        xiaomi_rooms = _build_xiaomi_rooms(self.coordinator.room_info or [])
        charger = meta.get("charger") if isinstance(meta.get("charger"), dict) else None
        charger_position = None
        if charger and charger.get("x") is not None and charger.get("y") is not None:
            charger_position = {
                "x": _scale_coord(charger.get("x")),
                "y": _scale_coord(charger.get("y")),
                "a": charger.get("angle") if charger.get("angle") is not None else charger.get("a"),
            }
        scaled_calibration_points = _scale_calibration_points(meta.get("calibration_points") or [])
        attrs = {
            "map_png_ready": self.coordinator.map_png_bytes is not None,
            "last_map_refresh": self.coordinator.last_map_refresh,
            "active_map_id": data.get("active_map_id"),
            "active_map_version": data.get("active_map_version"),
            "map_archive_path": self.coordinator.map_archive_path,
            "auto_download_map": self.coordinator.auto_download_map,
            "s3_map_url_known": data.get("s3_map_url_known"),
            "rooms": xiaomi_rooms,
            "selected_room": self.coordinator.selected_room,
            "debug_dir": data.get("debug_dir"),
            "umf_fetched": data.get("umf_fetched"),
            "map_render_metadata": meta,
            "calibration_points": scaled_calibration_points,
            "charger": charger,
            "charger_position": _scale_point_dict(display_charger_position) if display_charger_position else None,
            "map_data": map_data,
            "rotation": 0,
            "virtual_walls": [],
            "virtual_thresholds": [],
            "no_go_areas": [],
            "no_mopping_areas": [],
            "carpets": [],
            "deleted_carpets": [],
            "map_id": data.get("active_map_id"),
            "frame_id": live_state.get("timestamp") if isinstance(live_state, dict) else None,
            "map_index": 1,
            "is_empty": False,
        }
        if meta.get("image"):
            attrs["image"] = meta.get("image")
        if display_vacuum_position:
            scaled_vac = _scale_point_dict(display_vacuum_position) or {}
            if scaled_vac:
                attrs["vacuum_position"] = scaled_vac
                attrs["robot_position"] = {**scaled_vac, "angle": display_vacuum_position.get("angle"), "theta": display_vacuum_position.get("theta")}
                attrs["pose_source"] = status.get("pose_source")
                attrs["effective_robot_position_mode"] = (
                    "charger" if charger_position and scaled_vac.get("x") == charger_position.get("x") and scaled_vac.get("y") == charger_position.get("y")
                    else "return_path" if _is_returning_state(vacuum_state, phase) and bool(display_return_path_points)
                    else "last_active" if getattr(self, "_last_active_vacuum_position", None) and _is_returning_state(vacuum_state, phase)
                    else "live"
                )
        attrs["display_position_mode"] = display_position_mode
        attrs["debug_vacuum_state"] = vacuum_state
        attrs["debug_mission_phase"] = phase
        attrs["display_offset"] = {"x": DISPLAY_OFFSET_X, "y": DISPLAY_OFFSET_Y}
        attrs["return_path_points_count"] = len(display_return_path_points)
        if display_return_path_points:
            attrs["return_path"] = {"point_length": len(display_return_path_points), "point_size": 2, "angle": 0, "path": [[pt.get("x"), pt.get("y")] for pt in display_return_path_points]}
        attrs["path_points_count"] = len(display_path_points)
        attrs["has_path"] = bool(card_path and card_path.get("path"))
        attrs["local_path_points_count"] = len(self._local_path_points)
        attrs["recent_pose_candidates_count"] = len(self._recent_pose_candidates)
        if card_path and card_path.get("path"):
            attrs["path"] = card_path
        return attrs
