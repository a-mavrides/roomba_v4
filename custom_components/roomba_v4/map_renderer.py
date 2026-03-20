from __future__ import annotations

import io
import math
import json
import struct
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def extract_room_info_from_archive(archive_path: str | Path) -> list[dict[str, Any]]:
    archive = Path(archive_path)
    with tempfile.TemporaryDirectory(prefix="roomba_v4_rooms_") as tmp:
        tmpdir = Path(tmp)
        with tarfile.open(archive, "r:gz") as tf:
            tf.extractall(tmpdir)
        rooms_path = tmpdir / "rooms.geojson"
        if not rooms_path.exists():
            return []
        rooms = json.loads(rooms_path.read_text(encoding="utf-8"))
        out: list[dict[str, Any]] = []
        for idx, feat in enumerate(rooms.get("features", []), start=1):
            props = feat.get("properties", {}) or {}
            label = props.get("name") or props.get("title") or props.get("label") or props.get("id") or f"Room {idx}"
            room_id = props.get("id") or props.get("room_id") or props.get("region_id") or props.get("segment_id") or str(idx)
            out.append({
                "name": str(label),
                "id": str(room_id),
                "properties": props,
                "geometry_type": (feat.get("geometry") or {}).get("type"),
            })
        return out


def room_names_from_info(room_info: list[dict[str, Any]]) -> list[str]:
    return [str(room.get("name")) for room in room_info if room.get("name")]


def extract_rooms_from_archive(archive_path: str | Path) -> list[str]:
    return room_names_from_info(extract_room_info_from_archive(archive_path))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def _features(obj: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not obj:
        return []
    if obj.get("type") == "FeatureCollection":
        return obj.get("features", [])
    if obj.get("type") == "Feature":
        return [obj]
    return []

def _coords(geom: dict[str, Any] | None) -> list[tuple[float, float]]:
    out = []
    if not geom:
        return out
    def walk(v: Any):
        if isinstance(v, (list, tuple)):
            if len(v) >= 2 and isinstance(v[0], (int, float)) and isinstance(v[1], (int, float)):
                out.append((float(v[0]), float(v[1])))
            else:
                for item in v:
                    walk(item)
    walk(geom.get("coordinates", []))
    return out


def _effective_geometry(feat: dict[str, Any] | None, *, prefer_simplified: bool = True) -> dict[str, Any] | None:
    if not feat:
        return None
    props = feat.get("properties", {}) or {}
    if prefer_simplified:
        simp = props.get("simplifiedGeometry")
        if isinstance(simp, dict) and simp.get("type") and simp.get("coordinates"):
            return simp
    geom = feat.get("geometry")
    return geom if isinstance(geom, dict) else None

def _bounds(*objs):
    pts = []
    for obj in objs:
        for feat in _features(obj):
            pts.extend(_coords(_effective_geometry(feat)))
    if not pts:
        return 0, 1, 0, 1
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return min(xs), max(xs), min(ys), max(ys)

def _plot_polygon(ax, rings, facecolor, edgecolor, alpha=1.0, linewidth=1.0):
    if not rings:
        return
    outer = rings[0]
    if outer:
        xs = [p[0] for p in outer]
        ys = [p[1] for p in outer]
        ax.fill(
            xs,
            ys,
            facecolor=facecolor,
            edgecolor=edgecolor,
            alpha=alpha,
            linewidth=linewidth,
            joinstyle="miter",
            antialiased=False,
        )



def _cluster_axis_values(values, tolerance: float = 0.30, snap_step: float = 0.1) -> list[float]:
    vals = sorted(float(v) for v in values)
    if not vals:
        return []
    clusters: list[list[float]] = [[vals[0]]]
    for v in vals[1:]:
        current = clusters[-1]
        center = sum(current) / len(current)
        if abs(v - center) <= tolerance:
            current.append(v)
        else:
            clusters.append([v])

    representatives: list[float] = []
    for cluster in clusters:
        snapped = [_snap_value(v, snap_step) for v in cluster]
        counts: dict[float, int] = {}
        for sv in snapped:
            counts[sv] = counts.get(sv, 0) + 1
        best = max(counts.items(), key=lambda item: (item[1], -abs(item[0] - (sum(cluster) / len(cluster)))))[0]
        representatives.append(round(float(best), 3))
    return representatives


def _nearest_cluster(value: float, clusters: list[float], max_distance: float = 0.30) -> float:
    if not clusters:
        return value
    best = min(clusters, key=lambda c: abs(c - value))
    return best if abs(best - value) <= max_distance else value


def _collect_room_axis_clusters(rooms: dict[str, Any] | None, tolerance: float = 0.30) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    if not rooms:
        return xs, ys
    for feat in _features(rooms):
        geom = _effective_geometry(feat) or {}
        if geom.get("type") != "Polygon":
            continue
        for ring in geom.get("coordinates", []):
            for pt in ring:
                if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    xs.append(float(pt[0]))
                    ys.append(float(pt[1]))
    return _cluster_axis_values(xs, tolerance), _cluster_axis_values(ys, tolerance)


def _snap_value(value: float, step: float = 0.1) -> float:
    return math.floor((value / step) + 0.5) * step if value >= 0 else math.ceil((value / step) - 0.5) * step


def _is_axis_aligned(a, b, tol: float = 1e-9) -> bool:
    return abs(a[0] - b[0]) <= tol or abs(a[1] - b[1]) <= tol


def _remove_consecutive_duplicates(points, tol: float = 1e-9):
    if not points:
        return []
    out = [points[0]]
    for p in points[1:]:
        if abs(p[0] - out[-1][0]) > tol or abs(p[1] - out[-1][1]) > tol:
            out.append(p)
    return out


def _remove_collinear_axis_points(points, tol: float = 1e-9):
    if len(points) < 3:
        return points
    changed = True
    out = points[:]
    while changed and len(out) >= 3:
        changed = False
        new = []
        n = len(out)
        for i in range(n):
            prev = out[i - 1]
            curr = out[i]
            nxt = out[(i + 1) % n]
            same_x = abs(prev[0] - curr[0]) <= tol and abs(curr[0] - nxt[0]) <= tol
            same_y = abs(prev[1] - curr[1]) <= tol and abs(curr[1] - nxt[1]) <= tol
            if same_x or same_y:
                changed = True
                continue
            new.append(curr)
        if len(new) < 3:
            break
        out = new
    return out


def _prune_short_axis_segments(points, min_len: float = 0.2, tol: float = 1e-9):
    if len(points) < 4:
        return points
    out = points[:]
    changed = True
    while changed and len(out) >= 4:
        changed = False
        new = []
        n = len(out)
        for i in range(n):
            prev = out[i - 1]
            curr = out[i]
            nxt = out[(i + 1) % n]
            seg_len = max(abs(curr[0] - prev[0]), abs(curr[1] - prev[1]))
            if seg_len >= (min_len - tol):
                new.append(curr)
                continue
            # only drop tiny segment points when replacement keeps orthogonal path stable
            cand1 = (prev[0], nxt[1])
            cand2 = (nxt[0], prev[1])
            keep = None
            if _is_axis_aligned(prev, cand1, tol) and _is_axis_aligned(cand1, nxt, tol):
                keep = cand1
            elif _is_axis_aligned(prev, cand2, tol) and _is_axis_aligned(cand2, nxt, tol):
                keep = cand2
            if keep is not None:
                if new and abs(new[-1][0] - keep[0]) <= tol and abs(new[-1][1] - keep[1]) <= tol:
                    pass
                else:
                    new.append(keep)
                changed = True
            else:
                new.append(curr)
        if len(new) < 3:
            break
        out = _remove_consecutive_duplicates(new, tol)
        out = _remove_collinear_axis_points(out, tol)
    return out



def _segment_axis(a, b, tol: float = 1e-9):
    dx = b[0] - a[0]
    dy = b[1] - a[1]
    if abs(dx) <= tol and abs(dy) <= tol:
        return None
    return "h" if abs(dx) >= abs(dy) else "v"


def _collapse_alternating_stair_runs(points, snap_step: float = 0.1, tol: float = 1e-9):
    """Collapse small vector staircases into one orthogonal corner.

    This targets runs like H-V-H-V or V-H-V-H that stay within a small
    bounding box after grid snapping. It is intentionally conservative so it
    only rewrites localized staircase artifacts.
    """
    if len(points) < 5:
        return points

    max_box = snap_step * 8.5
    max_seg = snap_step * 3.5
    out = points[:]
    changed = True
    while changed and len(out) >= 5:
        changed = False
        n = len(out)
        i = 0
        rebuilt = []
        while i < n:
            start = i
            j = i + 1
            prev_axis = _segment_axis(out[start], out[j % n], tol)
            if prev_axis is None:
                rebuilt.append(out[i])
                i += 1
                continue

            xs = [out[start][0], out[j % n][0]]
            ys = [out[start][1], out[j % n][1]]
            seg_lens = [max(abs(out[j % n][0]-out[start][0]), abs(out[j % n][1]-out[start][1]))]
            x_signs = []
            y_signs = []
            k = j
            while k + 1 < n + start:
                a = out[k % n]
                b = out[(k + 1) % n]
                axis = _segment_axis(a, b, tol)
                if axis is None or axis == prev_axis:
                    break
                seg_len = max(abs(b[0]-a[0]), abs(b[1]-a[1]))
                if seg_len > max_seg:
                    break
                dx = b[0] - a[0]
                dy = b[1] - a[1]
                if abs(dx) > tol:
                    x_signs.append(1 if dx > 0 else -1)
                if abs(dy) > tol:
                    y_signs.append(1 if dy > 0 else -1)
                xs.append(b[0]); ys.append(b[1]); seg_lens.append(seg_len)
                prev_axis = axis
                k += 1

            run_pts = [out[idx % n] for idx in range(start, k + 1)]
            if len(run_pts) >= 5:
                minx,maxx=min(xs),max(xs)
                miny,maxy=min(ys),max(ys)
                mono_x = len(set(x_signs)) <= 1
                mono_y = len(set(y_signs)) <= 1
                if mono_x and mono_y and (maxx-minx) <= max_box and (maxy-miny) <= max_box:
                    first = run_pts[0]
                    last = run_pts[-1]
                    cands = []
                    cand1=(first[0], last[1])
                    cand2=(last[0], first[1])
                    for cand in (cand1,cand2):
                        if _is_axis_aligned(first,cand,tol) and _is_axis_aligned(cand,last,tol):
                            # choose candidate closest to staircase centroid so the shape shift is smaller
                            cx = sum(x for x,_ in run_pts) / len(run_pts)
                            cy = sum(y for _,y in run_pts) / len(run_pts)
                            score = abs(cand[0]-cx) + abs(cand[1]-cy)
                            cands.append((score,cand))
                    if cands:
                        if not rebuilt or (abs(rebuilt[-1][0]-first[0]) > tol or abs(rebuilt[-1][1]-first[1]) > tol):
                            rebuilt.append(first)
                        rebuilt.append(sorted(cands, key=lambda t: t[0])[0][1])
                        i = k
                        changed = True
                        continue

            rebuilt.append(out[i])
            i += 1

        out = _remove_consecutive_duplicates(rebuilt, tol)
        out = _remove_collinear_axis_points(out, tol)
    return out



def _axis_dir(a, b, tol: float = 1e-9):
    dx = b[0] - a[0]
    dy = b[1] - a[1]
    if abs(dx) <= tol and abs(dy) <= tol:
        return None, 0
    if abs(dx) >= abs(dy):
        return "h", (1 if dx > 0 else -1)
    return "v", (1 if dy > 0 else -1)


def _remove_small_rectilinear_features(points, snap_step: float = 0.1, tol: float = 1e-9):
    """Remove narrow orthogonal notches/extrusions while preserving main walls.

    Targets 5-segment runs A-B-A-B-A where the first/third/fifth segments keep
    moving in the same direction and the middle B segments briefly step out and
    back. This matches the small bites visible in MQTT room geometry.
    """
    if len(points) < 6:
        return points

    depth_limit = snap_step * 4.5   # ~0.45 m at 0.1 grid
    span_limit = snap_step * 12.0   # keep local only

    out = points[:]
    changed = True
    while changed and len(out) >= 6:
        changed = False
        n = len(out)
        rebuilt = []
        i = 0
        while i < n:
            if i + 5 < n:
                p0, p1, p2, p3, p4, p5 = out[i:i+6]
                a1, d1 = _axis_dir(p0, p1, tol)
                a2, d2 = _axis_dir(p1, p2, tol)
                a3, d3 = _axis_dir(p2, p3, tol)
                a4, d4 = _axis_dir(p3, p4, tol)
                a5, d5 = _axis_dir(p4, p5, tol)
                axes_ok = a1 and a2 and a3 and a4 and a5 and a1 == a3 == a5 and a2 == a4 and a1 != a2
                if axes_ok:
                    same_main_dir = d1 == d3 == d5
                    opposite_cross = d2 == -d4
                    p0_p5_aligned = (a1 == 'h' and abs(p0[1] - p5[1]) <= tol) or (a1 == 'v' and abs(p0[0] - p5[0]) <= tol)
                    xs = [p[0] for p in (p0,p1,p2,p3,p4,p5)]
                    ys = [p[1] for p in (p0,p1,p2,p3,p4,p5)]
                    width = max(xs) - min(xs)
                    height = max(ys) - min(ys)
                    depth = min(width, height)
                    span = max(width, height)
                    if same_main_dir and opposite_cross and p0_p5_aligned and depth <= depth_limit and span <= span_limit:
                        if not rebuilt or (abs(rebuilt[-1][0]-p0[0]) > tol or abs(rebuilt[-1][1]-p0[1]) > tol):
                            rebuilt.append(p0)
                        rebuilt.append(p5)
                        i += 5
                        changed = True
                        continue
            rebuilt.append(out[i])
            i += 1

        out = _remove_consecutive_duplicates(rebuilt, tol)
        out = _remove_collinear_axis_points(out, tol)
    return out

def _normalize_axis_chain(points, tol: float = 1e-9):
    if len(points) < 2:
        return points[:]
    out = [points[0]]
    for p in points[1:]:
        prev = out[-1]
        dx = p[0] - prev[0]
        dy = p[1] - prev[1]
        if abs(dx) <= tol and abs(dy) <= tol:
            continue
        if abs(dx) <= abs(dy):
            out.append((prev[0], p[1]))
        else:
            out.append((p[0], prev[1]))
    return out




def _straighten_segment_runs(points, x_clusters: list[float] | None = None, y_clusters: list[float] | None = None, tol: float = 1e-9):
    if len(points) < 3:
        return points[:]
    out = points[:]
    n = len(out)

    def choose_value(values, clusters, step=0.1):
        if not values:
            return None
        avg = sum(values) / len(values)
        if clusters:
            near = [c for c in clusters if abs(c - avg) <= step * 2.5]
            if near:
                return min(near, key=lambda c: abs(c - avg))
        return _snap_value(avg, step)

    # Straighten local 3-point wall runs so tiny wobble points fall onto one wall line.
    changed = True
    while changed and len(out) >= 3:
        changed = False
        n = len(out)
        new = out[:]
        for i in range(n):
            prev = new[i - 1]
            curr = new[i]
            nxt = new[(i + 1) % n]
            # vertical-ish wall around curr
            if abs(prev[0] - curr[0]) <= 0.35 and abs(curr[0] - nxt[0]) <= 0.35:
                target_x = choose_value([prev[0], curr[0], nxt[0]], x_clusters)
                if target_x is not None and (abs(prev[0] - target_x) <= 0.35 and abs(curr[0] - target_x) <= 0.35 and abs(nxt[0] - target_x) <= 0.35):
                    if new[i - 1][0] != target_x or new[i][0] != target_x or new[(i + 1) % n][0] != target_x:
                        new[i - 1] = (target_x, new[i - 1][1])
                        new[i] = (target_x, new[i][1])
                        new[(i + 1) % n] = (target_x, new[(i + 1) % n][1])
                        changed = True
            # horizontal-ish wall around curr
            if abs(prev[1] - curr[1]) <= 0.35 and abs(curr[1] - nxt[1]) <= 0.35:
                target_y = choose_value([prev[1], curr[1], nxt[1]], y_clusters)
                if target_y is not None and (abs(prev[1] - target_y) <= 0.35 and abs(curr[1] - target_y) <= 0.35 and abs(nxt[1] - target_y) <= 0.35):
                    if new[i - 1][1] != target_y or new[i][1] != target_y or new[(i + 1) % n][1] != target_y:
                        new[i - 1] = (new[i - 1][0], target_y)
                        new[i] = (new[i][0], target_y)
                        new[(i + 1) % n] = (new[(i + 1) % n][0], target_y)
                        changed = True
        out = _remove_consecutive_duplicates(new, tol)
        out = _remove_collinear_axis_points(out, tol)
    return out

def _ring_is_safe(points, tol: float = 1e-9):
    if len(points) < 4:
        return False
    if abs(points[0][0] - points[-1][0]) > tol or abs(points[0][1] - points[-1][1]) > tol:
        return False
    unique = {(round(x, 6), round(y, 6)) for x, y in points[:-1]}
    if len(unique) < 3:
        return False
    for a, b in zip(points, points[1:]):
        if not _is_axis_aligned(a, b, tol):
            return False
        if abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol:
            return False
    return True


def _clean_room_ring(ring, snap_step: float = 0.1, x_clusters: list[float] | None = None, y_clusters: list[float] | None = None):
    if not ring:
        return ring
    closed = len(ring) > 1 and ring[0] == ring[-1]
    pts = ring[:-1] if closed else ring[:]

    base = []
    for x, y in pts:
        sx = _snap_value(float(x), snap_step)
        sy = _snap_value(float(y), snap_step)
        if x_clusters:
            sx = _nearest_cluster(sx, x_clusters, max_distance=snap_step * 3.0)
        if y_clusters:
            sy = _nearest_cluster(sy, y_clusters, max_distance=snap_step * 3.0)
        base.append((sx, sy))

    fallback = _remove_consecutive_duplicates(base)
    fallback = _remove_collinear_axis_points(fallback)
    if closed and fallback and fallback[0] != fallback[-1]:
        fallback.append(fallback[0])

    snapped = fallback[:-1] if closed and fallback and fallback[0] == fallback[-1] else fallback[:]
    if len(snapped) >= 3:
        repaired = []
        n = len(snapped)
        for i in range(n):
            prev = snapped[i - 1]
            curr = snapped[i]
            nxt = snapped[(i + 1) % n]
            if _is_axis_aligned(prev, curr) and _is_axis_aligned(curr, nxt):
                repaired.append(curr)
                continue
            d1 = max(abs(curr[0] - prev[0]), abs(curr[1] - prev[1]))
            d2 = max(abs(nxt[0] - curr[0]), abs(nxt[1] - curr[1]))
            if d1 <= (snap_step * 1.5) and d2 <= (snap_step * 1.5):
                corner1 = (prev[0], nxt[1])
                corner2 = (nxt[0], prev[1])
                cand = corner1 if _is_axis_aligned(prev, corner1) and _is_axis_aligned(corner1, nxt) else corner2
                if _is_axis_aligned(prev, cand) and _is_axis_aligned(cand, nxt):
                    if not repaired or repaired[-1] != cand:
                        repaired.append(cand)
                    continue
            repaired.append(curr)
        snapped = _remove_consecutive_duplicates(repaired)
        snapped = _remove_collinear_axis_points(snapped)
        snapped = _collapse_alternating_stair_runs(snapped, snap_step)
        snapped = _remove_small_rectilinear_features(snapped, snap_step)
        snapped = _normalize_axis_chain(snapped)
        snapped = _straighten_segment_runs(snapped, x_clusters=x_clusters, y_clusters=y_clusters)
        snapped = _prune_short_axis_segments(snapped, min_len=snap_step * 2.0)
        snapped = _straighten_segment_runs(snapped, x_clusters=x_clusters, y_clusters=y_clusters)
        snapped = _remove_consecutive_duplicates(snapped)
        snapped = _remove_collinear_axis_points(snapped)

    cleaned = snapped[:]
    if closed and cleaned and cleaned[0] != cleaned[-1]:
        cleaned.append(cleaned[0])

    if _ring_is_safe(cleaned):
        return cleaned
    return fallback if _ring_is_safe(fallback) else ring


def _clean_room_rings(rings, x_clusters: list[float] | None = None, y_clusters: list[float] | None = None):
    if not rings:
        return rings
    cleaned = []
    for ring in rings:
        cleaned.append(_clean_room_ring(ring, x_clusters=x_clusters, y_clusters=y_clusters))
    return cleaned

def _ring_center(ring):
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    return sum(xs)/len(xs), sum(ys)/len(ys)



def _png_size(png_bytes: bytes) -> tuple[int, int] | None:
    if not png_bytes or len(png_bytes) < 24 or png_bytes[:8] != b"\x89PNG\r\n\x1a\n":
        return None
    width, height = struct.unpack(">II", png_bytes[16:24])
    return int(width), int(height)


def extract_map_render_metadata(archive_path: str | Path, png_bytes: bytes | None = None) -> dict[str, Any]:
    archive = Path(archive_path)
    with tempfile.TemporaryDirectory(prefix="roomba_v4_meta_") as tmp:
        tmpdir = Path(tmp)
        with tarfile.open(archive, "r:gz") as tf:
            tf.extractall(tmpdir)

        rooms = _load_json(tmpdir / "rooms.geojson") if (tmpdir / "rooms.geojson").exists() else None
        borders = _load_json(tmpdir / "borders.geojson") if (tmpdir / "borders.geojson").exists() else None
        policy = _load_json(tmpdir / "policyZones.geojson") if (tmpdir / "policyZones.geojson").exists() else None
        trajectories = _load_json(tmpdir / "trajectories.geojson") if (tmpdir / "trajectories.geojson").exists() else None
        coverage = _load_json(tmpdir / "coverage.geojson") if (tmpdir / "coverage.geojson").exists() else None
        dock = _load_json(tmpdir / "dockPose.geojson") if (tmpdir / "dockPose.geojson").exists() else None

        minx, maxx, miny, maxy = _bounds(rooms, borders, policy, trajectories, coverage, dock)
        dx = max(maxx - minx, 1)
        dy = max(maxy - miny, 1)
        padx = dx * 0.05
        pady = dy * 0.05
        render_minx = minx - padx
        render_maxx = maxx + padx
        render_miny = miny - pady
        render_maxy = maxy + pady
        out: dict[str, Any] = {
            "bounds": {"min_x": minx, "max_x": maxx, "min_y": miny, "max_y": maxy},
            "render_bounds": {"min_x": render_minx, "max_x": render_maxx, "min_y": render_miny, "max_y": render_maxy},
            "padding": {"x": padx, "y": pady},
        }

        size = _png_size(png_bytes or b"") if png_bytes else None
        if size:
            width, height = size
            out["image"] = {"width": width, "height": height}
            out["calibration_points"] = [
                {"map": {"x": 0, "y": height}, "vacuum": {"x": render_minx, "y": render_miny}},
                {"map": {"x": width, "y": height}, "vacuum": {"x": render_maxx, "y": render_miny}},
                {"map": {"x": 0, "y": 0}, "vacuum": {"x": render_minx, "y": render_maxy}},
            ]

        dock_features = _features(dock)
        for feat in dock_features:
            geom = feat.get("geometry", {}) or {}
            if geom.get("type") == "Point":
                coords = geom.get("coordinates", [])
                if isinstance(coords, list) and len(coords) >= 2:
                    out["charger"] = {"x": float(coords[0]), "y": float(coords[1])}
                    props = feat.get("properties", {}) or {}
                    angle = props.get("theta") or props.get("angle")
                    if isinstance(angle, (int, float)):
                        out["charger"]["a"] = float(angle)
                    break

        return out

def render_archive_to_png_bytes(archive_path: str | Path, show_labels: bool = True, show_coverage: bool = True) -> bytes:
    archive = Path(archive_path)
    with tempfile.TemporaryDirectory(prefix="roomba_v4_map_") as tmp:
        tmpdir = Path(tmp)
        with tarfile.open(archive, "r:gz") as tf:
            tf.extractall(tmpdir)

        rooms = _load_json(tmpdir / "rooms.geojson") if (tmpdir / "rooms.geojson").exists() else None
        borders = _load_json(tmpdir / "borders.geojson") if (tmpdir / "borders.geojson").exists() else None
        policy = _load_json(tmpdir / "policyZones.geojson") if (tmpdir / "policyZones.geojson").exists() else None
        trajectories = _load_json(tmpdir / "trajectories.geojson") if (tmpdir / "trajectories.geojson").exists() else None
        coverage = _load_json(tmpdir / "coverage.geojson") if (tmpdir / "coverage.geojson").exists() else None
        dock = _load_json(tmpdir / "dockPose.geojson") if (tmpdir / "dockPose.geojson").exists() else None

        minx, maxx, miny, maxy = _bounds(rooms, borders, policy, trajectories, coverage, dock)
        dx = max(maxx - minx, 1)
        dy = max(maxy - miny, 1)
        padx = dx * 0.05
        pady = dy * 0.05

        fig, ax = plt.subplots(figsize=(10, max(6, 10 * (dy / dx) if dx else 6)), dpi=150)
        ax.set_facecolor("#f8fafc")

        if show_coverage and coverage:
            for feat in _features(coverage):
                geom = feat.get("geometry", {})
                if geom.get("type") == "Polygon":
                    _plot_polygon(ax, geom.get("coordinates", []), "#dbeafe", "#bfdbfe", alpha=0.5, linewidth=0.5)

        room_x_clusters, room_y_clusters = _collect_room_axis_clusters(rooms, tolerance=0.25)

        palette = ["#eef2ff", "#ecfeff", "#f0fdf4", "#fff7ed", "#fef2f2", "#faf5ff", "#fefce8"]
        if rooms:
            for idx, feat in enumerate(_features(rooms), start=1):
                geom = _effective_geometry(feat) or {}
                if geom.get("type") == "Polygon":
                    rings = _clean_room_rings(geom.get("coordinates", []), x_clusters=room_x_clusters, y_clusters=room_y_clusters)
                    _plot_polygon(ax, rings, palette[(idx - 1) % len(palette)], "#94a3b8", alpha=0.95, linewidth=1.0)
                    # Room labels intentionally disabled to avoid duplicate/small labels
                    # on the Lovelace map card. Keep the geometry rendering only.

        if borders:
            for feat in _features(borders):
                geom = feat.get("geometry", {})
                if geom.get("type") == "LineString":
                    line = geom.get("coordinates", [])
                    ax.plot(
                        [p[0] for p in line],
                        [p[1] for p in line],
                        color="#0f172a",
                        linewidth=1.2,
                        solid_joinstyle="miter",
                        solid_capstyle="butt",
                        antialiased=False,
                    )

        if policy:
            for feat in _features(policy):
                geom = feat.get("geometry", {})
                if geom.get("type") == "Polygon":
                    _plot_polygon(ax, geom.get("coordinates", []), "#fecaca", "#ef4444", alpha=0.3, linewidth=1.0)

        if trajectories:
            for feat in _features(trajectories):
                geom = feat.get("geometry", {})
                if geom.get("type") == "LineString":
                    line = geom.get("coordinates", [])
                    ax.plot(
                        [p[0] for p in line],
                        [p[1] for p in line],
                        color="#2563eb",
                        linewidth=1.1,
                        solid_joinstyle="miter",
                        solid_capstyle="butt",
                        antialiased=False,
                    )

        if dock:
            for feat in _features(dock):
                geom = feat.get("geometry", {})
                if geom.get("type") == "Point":
                    pt = geom.get("coordinates", [None, None])
                    if pt[0] is not None and pt[1] is not None:
                        ax.scatter([pt[0]], [pt[1]], s=80, marker="s", color="#111827")
                        ax.text(pt[0], pt[1], " Dock", fontsize=8, color="#111827", va="bottom")

        ax.set_xlim(minx - padx, maxx + padx)
        ax.set_ylim(miny - pady, maxy + pady)
        ax.set_aspect("equal", adjustable="box")
        ax.axis("off")
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        return buf.getvalue()
