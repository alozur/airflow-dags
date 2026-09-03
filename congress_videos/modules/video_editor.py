"""Video overlay editor module for drawtext and Pillow-based video editing.

Provides pure builder functions for constructing ffmpeg drawtext commands,
Pillow-based PNG overlay renderers, and orchestration functions for source
resolution, input validation, and running ffmpeg via subprocess.

Public API::

    _escape_drawtext(text)                        — escape ffmpeg drawtext metacharacters
    _parse_time(v)                                — normalise seconds value or SRT string
    build_drawtext_filter(overlays, domain_cfg)   → str
    build_ffmpeg_drawtext_cmd(src, out, filter)   → list[str]
    build_ffmpeg_pillow_cmd(src, out, png_slots)  → list[str]
    render_pillow_overlay(overlay, domain_cfg, W, H) → PIL.Image
    _resolve_source_path(conf)                    → str
    _default_output_path(source_path)             → str
    validate_editor_input(conf)
    apply_overlays(source_path, output_path, overlays, domain_cfg) → dict
"""

from __future__ import annotations

import functools
import logging
import os
import subprocess
import tempfile
from pathlib import Path

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.video_editor_config import get_domain_config
from congress_videos.modules.video_splitter import (
    compute_ffmpeg_timeout,
    convert_srt_time_to_seconds,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure helper: drawtext escaping
# ---------------------------------------------------------------------------


def _escape_drawtext(text: str) -> str:
    """Escape *text* for safe embedding inside an ffmpeg drawtext filter.

    ffmpeg's drawtext option parser is sensitive to several metacharacters.
    The escaping order matters — backslash must be escaped first so that
    subsequent replacements do not double-escape newly inserted backslashes.

    Escape order:
        1. ``\\``  → ``\\\\``
        2. ``%``   → ``\\%``
        3. ``:``   → ``\\:``
        4. ``'``   → ``\\'``
        5. ``{``   → ``\\{``
        6. ``}``   → ``\\}``
        7. newline → ``\\n``

    Spanish characters (á, é, í, ó, ú, ñ, Ñ, ¿, ¡, etc.) are valid UTF-8
    and pass through unescaped — freetype renders them directly.

    Args:
        text: Raw overlay text string.

    Returns:
        Escaped string safe for inclusion in a drawtext ``text='...'`` clause.
    """
    text = text.replace("\\", "\\\\")
    text = text.replace("%", "\\%")
    text = text.replace(":", "\\:")
    text = text.replace("'", "\\'")
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")
    text = text.replace("\n", "\\n")
    return text


# ---------------------------------------------------------------------------
# Pure helper: time normalisation
# ---------------------------------------------------------------------------


def _parse_time(v: int | float | str) -> float:
    """Normalise a time value to float seconds.

    Args:
        v: Seconds as ``int`` or ``float``, or an SRT timestamp string in
           ``HH:MM:SS,mmm`` or ``HH:MM:SS`` format.

    Returns:
        Time in seconds as ``float``.
    """
    if isinstance(v, (int, float)):
        return float(v)
    return convert_srt_time_to_seconds(str(v))


# ---------------------------------------------------------------------------
# Pure builder: drawtext filter string
# ---------------------------------------------------------------------------


def build_drawtext_filter(overlays: list[dict], domain_cfg: dict) -> str:
    """Build a comma-joined ffmpeg drawtext filter string for *overlays*.

    Each overlay is looked up by ``tipo`` in ``domain_cfg["tipos"]``. Text is
    built from ``titulo`` + optional ``descripcion`` joined with the drawtext
    newline escape (``\\n``), then passed through :func:`_escape_drawtext`.

    Args:
        overlays: List of overlay dicts with keys ``tipo``, ``tiempo_inicio``,
            ``tiempo_fin``, ``titulo``, and optional ``descripcion``.
        domain_cfg: Domain config dict (from :func:`get_domain_config`) with a
            ``tipos`` sub-dict mapping tipo names to style dicts.

    Returns:
        Comma-joined drawtext filter string suitable for ffmpeg ``-vf``.

    Raises:
        KeyError: When an overlay's ``tipo`` is not in ``domain_cfg["tipos"]``.
    """
    clauses: list[str] = []
    tipos_cfg: dict = domain_cfg["tipos"]

    for overlay in overlays:
        tipo = overlay["tipo"]
        if tipo not in tipos_cfg:
            raise KeyError(f"Unknown tipo {tipo!r}. Available tipos: {list(tipos_cfg.keys())}")
        style = tipos_cfg[tipo]

        # Build combined text (titulo + optional descripcion)
        raw_text = overlay["titulo"]
        if overlay.get("descripcion"):
            raw_text = raw_text + "\n" + overlay["descripcion"]
        escaped_text = _escape_drawtext(raw_text)

        start = _parse_time(overlay["tiempo_inicio"])
        end = _parse_time(overlay["tiempo_fin"])

        clause = (
            f"drawtext="
            f"fontfile={style['fontfile']}"
            f":text='{escaped_text}'"
            f":x={style['x']}"
            f":y={style['y']}"
            f":fontsize={style['fontsize']}"
            f":fontcolor={style['fontcolor']}"
            f":box={style['box']}"
            f":boxcolor={style['boxcolor']}"
            f":boxborderw={style['boxborderw']}"
            f":enable='between(t,{start},{end})'"
        )
        clauses.append(clause)

    return ",".join(clauses)


# ---------------------------------------------------------------------------
# Pure builder: ffmpeg argv list
# ---------------------------------------------------------------------------


def build_ffmpeg_drawtext_cmd(src: str, out: str, filter_str: str) -> list[str]:
    """Build an ffmpeg drawtext command as a pure argv list.

    The source file is never mutated; the output is written to a new path.
    Video is re-encoded with libx264/veryfast/crf20; audio is stream-copied.

    Args:
        src: Absolute path to the source video.
        out: Absolute path where the edited video will be written.
        filter_str: Comma-joined drawtext filter string (from
            :func:`build_drawtext_filter`).

    Returns:
        Argv list ready for ``subprocess.run``.  No shell string, no
        ``shell=True``.
    """
    return [
        "ffmpeg",
        "-y",
        "-i",
        src,
        "-vf",
        filter_str,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "copy",
        out,
    ]


# ---------------------------------------------------------------------------
# Source resolution helpers
# ---------------------------------------------------------------------------


def _resolve_source_path(conf: dict) -> str:
    """Resolve the absolute path to the source video from *conf*.

    Either ``source_path`` is provided directly, or the path is derived from
    ``video_id`` + ``chapter_id`` using the standard chapter video layout:
    ``{PROJECT_DATA_DIR}/{video_id}/{chapter_id}/chapter_video.mp4``.

    Args:
        conf: DAG run conf dict.

    Returns:
        Absolute path to the source video file.

    Raises:
        FileNotFoundError: When the resolved path does not exist on disk.
    """
    if "source_path" in conf:
        path = conf["source_path"]
    else:
        video_id = conf["video_id"]
        chapter_id = conf["chapter_id"]
        path = f"{PROJECT_DATA_DIR}/{video_id}/{chapter_id}/chapter_video.mp4"

    if not os.path.exists(path):
        raise FileNotFoundError(f"Source video not found: {path}")
    return path


def _default_output_path(source_path: str) -> str:
    """Derive a default output path by appending ``_edited`` before the extension.

    Example: ``/data/V1/C1/chapter_video.mp4`` → ``/data/V1/C1/chapter_video_edited.mp4``

    Args:
        source_path: Absolute path to the source video.

    Returns:
        Absolute path for the edited output video in the same directory.
    """
    p = Path(source_path)
    return str(p.parent / f"{p.stem}_edited{p.suffix}")


# ---------------------------------------------------------------------------
# Pillow overlay renderers
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=32)
def _load_font(path: str, size: int):
    """Load a TrueType font; fall back to Pillow default on failure."""
    from PIL import ImageFont

    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def _draw_text_block(
    draw,
    tx: int,
    box_y: int,
    box_h: int,
    title: str,
    sub: str | None,
    font_t,
    font_s,
    title_color: tuple,
    sub_color: tuple,
    gap: int = 8,
) -> None:
    """Draw a vertically-centered title + optional subtitle inside a box."""
    tb = draw.textbbox((0, 0), title, font=font_t)
    th_t = tb[3] - tb[1]
    if sub:
        sb = draw.textbbox((0, 0), sub, font=font_s)
        th_s = sb[3] - sb[1]
        ty = box_y + (box_h - th_t - gap - th_s) // 2
        draw.text((tx, ty - tb[1]), title, font=font_t, fill=title_color)
        draw.text((tx, ty + th_t + gap - sb[1]), sub, font=font_s, fill=sub_color)
    else:
        ty = box_y + (box_h - th_t) // 2
        draw.text((tx, ty - tb[1]), title, font=font_t, fill=title_color)


def _render_speaker_id(overlay: dict, style: dict, W: int, H: int):
    """Lower-left card: speaker name + role/party."""
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    bw = int(W * style["width_pct"])
    bh = style["height"]
    bx, by = style["margin_x"], H - bh - style["margin_y"]

    draw.rectangle([bx, by, bx + bw, by + bh], fill=style["bg_color"])
    draw.rectangle([bx, by, bx + bw, by + 6], fill=style["accent_color"])
    draw.rectangle([bx, by, bx + 10, by + bh], fill=style["accent_color"])

    _draw_text_block(
        draw,
        tx=bx + 22,
        box_y=by,
        box_h=bh,
        title=overlay["titulo"],
        sub=overlay.get("descripcion"),
        font_t=_load_font(style["fontfile"], style["fontsize_title"]),
        font_s=_load_font(style["fontfile_sub"], style["fontsize_sub"]),
        title_color=style["title_color"],
        sub_color=style["sub_color"],
        gap=10,
    )
    return img


def _render_cita_destacada(overlay: dict, style: dict, W: int, H: int):
    """Centered large quote box."""
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    font_t = _load_font(style["fontfile"], style["fontsize_title"])
    font_s = _load_font(style["fontfile_sub"], style["fontsize_sub"])
    pad = style["padding"]
    quote_text = f"“{overlay['titulo']}”"
    bw = int(W * style["width_pct"])
    bx = (W - bw) // 2

    # Size the box dynamically from measured text heights.
    bbox_t = draw.textbbox((0, 0), quote_text, font=font_t)
    th_title = bbox_t[3] - bbox_t[1]
    th_sub = 0
    if overlay.get("descripcion"):
        bbox_s = draw.textbbox((0, 0), overlay["descripcion"], font=font_s)
        th_sub = bbox_s[3] - bbox_s[1] + 10

    bh = pad * 2 + th_title + th_sub + 6  # +6 accent bar
    by = H - bh - 70

    draw.rectangle([bx, by, bx + bw, by + 5], fill=style["accent_color"])
    draw.rectangle([bx, by + 5, bx + bw, by + bh], fill=style["bg_color"])

    tx = bx + pad
    draw.text((tx, by + 5 + pad - bbox_t[1]), quote_text, font=font_t, fill=style["title_color"])
    if overlay.get("descripcion"):
        draw.text(
            (tx, by + 5 + pad + th_title + 10 - bbox_s[1]),
            overlay["descripcion"],
            font=font_s,
            fill=style["sub_color"],
        )
    return img


def _render_urgente(overlay: dict, style: dict, W: int, H: int):
    """Top red breaking-news banner."""
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    bw = int(W * style["width_pct"])
    bx = (W - bw) // 2
    bh = style["height"]
    by = style["margin_y"]

    font_label = _load_font(style["fontfile"], style["fontsize_label"])
    font_t = _load_font(style["fontfile"], style["fontsize_title"])
    font_s = _load_font(style["fontfile_sub"], style["fontsize_sub"])

    label_text = style["label"]
    lbbox = draw.textbbox((0, 0), label_text, font=font_label)
    label_w = lbbox[2] - lbbox[0] + 28

    draw.rectangle([bx, by, bx + bw, by + bh], fill=style["bg_color"])
    draw.rectangle([bx, by, bx + label_w, by + bh], fill=style["label_bg_color"])

    label_ty = by + (bh - (lbbox[3] - lbbox[1])) // 2 - lbbox[1]
    draw.text((bx + 14, label_ty), label_text, font=font_label, fill=style["label_color"])

    _draw_text_block(
        draw,
        tx=bx + label_w + 18,
        box_y=by,
        box_h=bh,
        title=overlay["titulo"],
        sub=overlay.get("descripcion"),
        font_t=font_t,
        font_s=font_s,
        title_color=style["title_color"],
        sub_color=style["sub_color"],
        gap=4,
    )
    draw.rectangle([bx, by + bh - 4, bx + bw, by + bh], fill=style["accent_color"])
    return img


def _render_dato_contexto(overlay: dict, style: dict, W: int, H: int):
    """Bottom-right compact data/context box."""
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    font_h = _load_font(style["fontfile"], style["fontsize_header"])
    font_t = _load_font(style["fontfile"], style["fontsize_title"])
    font_s = _load_font(style["fontfile_sub"], style["fontsize_sub"])
    pad = style["padding"]
    bw = style["width"]

    header_label = style["label"]
    hbbox = draw.textbbox((0, 0), header_label, font=font_h)
    th_header = hbbox[3] - hbbox[1]
    tbbox = draw.textbbox((0, 0), overlay["titulo"], font=font_t)
    th_title = tbbox[3] - tbbox[1]
    th_sub = 0
    if overlay.get("descripcion"):
        sbbox = draw.textbbox((0, 0), overlay["descripcion"], font=font_s)
        th_sub = sbbox[3] - sbbox[1] + 8

    bh = pad * 2 + th_header + 8 + th_title + th_sub + 6
    bx = W - bw - style["margin_x"]
    by = H - bh - style["margin_y"]

    draw.rectangle([bx, by, bx + bw, by + bh], fill=style["bg_color"])
    draw.rectangle([bx, by, bx + bw, by + 5], fill=style["accent_color"])
    draw.rectangle([bx, by, bx + 6, by + bh], fill=style["accent_color"])

    tx, ty = bx + pad, by + pad
    draw.text((tx, ty - hbbox[1]), header_label, font=font_h, fill=style["header_color"])
    draw.text((tx, ty + th_header + 8 - tbbox[1]), overlay["titulo"], font=font_t, fill=style["title_color"])
    if overlay.get("descripcion"):
        draw.text(
            (tx, ty + th_header + 8 + th_title + 8 - sbbox[1]),
            overlay["descripcion"],
            font=font_s,
            fill=style["sub_color"],
        )
    return img


def _render_extracto_sesion(overlay: dict, style: dict, W: int, H: int):
    """Bottom-centered 90% bar: session extract label, same language as speaker_id."""
    from PIL import Image, ImageDraw

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    bw = int(W * style["width_pct"])
    bh = style["height"]
    bx = (W - bw) // 2
    by = H - bh - style["margin_y"]

    draw.rectangle([bx, by, bx + bw, by + bh], fill=style["bg_color"])
    draw.rectangle([bx, by, bx + bw, by + 6], fill=style["accent_color"])
    draw.rectangle([bx, by, bx + 10, by + bh], fill=style["accent_color"])

    _draw_text_block(
        draw,
        tx=bx + 22,
        box_y=by,
        box_h=bh,
        title=overlay["titulo"],
        sub=overlay.get("descripcion"),
        font_t=_load_font(style["fontfile"], style["fontsize_title"]),
        font_s=_load_font(style["fontfile_sub"], style["fontsize_sub"]),
        title_color=style["title_color"],
        sub_color=style["sub_color"],
        gap=10,
    )
    return img


_PILLOW_RENDERERS = {
    "extracto_sesion": _render_extracto_sesion,
    "speaker_id": _render_speaker_id,
    "cita_destacada": _render_cita_destacada,
    "urgente": _render_urgente,
    "dato_contexto": _render_dato_contexto,
}


def render_pillow_overlay(overlay: dict, domain_cfg: dict, W: int, H: int):
    """Dispatch to the correct Pillow renderer for *overlay["tipo"]*.

    Args:
        overlay: Overlay dict with ``tipo``, ``titulo``, optional ``descripcion``.
        domain_cfg: Domain config dict from :func:`get_domain_config`.
        W: Video frame width in pixels.
        H: Video frame height in pixels.

    Returns:
        RGBA ``PIL.Image`` the size of the video frame.

    Raises:
        KeyError: When the tipo has no registered Pillow renderer.
    """
    tipo = overlay["tipo"]
    style = domain_cfg["tipos"][tipo]
    renderer_fn = _PILLOW_RENDERERS.get(tipo)
    if renderer_fn is None:
        raise KeyError(f"No Pillow renderer registered for tipo {tipo!r}.")
    return renderer_fn(overlay, style, W, H)


# ---------------------------------------------------------------------------
# Pillow ffmpeg command builder
# ---------------------------------------------------------------------------


def build_ffmpeg_pillow_cmd(
    src: str,
    out: str,
    png_slots: list[tuple[str, float, float]],
) -> list[str]:
    """Build an ffmpeg command that composites timed PNG overlays onto *src*.

    Each PNG covers the full video frame (transparent background); timing is
    controlled via ffmpeg's ``enable='between(t,start,end)'`` expression.
    Overlays are chained sequentially in filter_complex.

    Args:
        src: Absolute path to the source video.
        out: Absolute path for the output video.
        png_slots: List of ``(png_path, t_start, t_end)`` tuples in order.

    Returns:
        Argv list ready for ``subprocess.run``.
    """
    cmd = ["ffmpeg", "-y", "-i", src]
    for png_path, _, _ in png_slots:
        cmd += ["-i", png_path]

    # Build filter_complex chain
    parts: list[str] = []
    prev = "0:v"
    for idx, (_, t_start, t_end) in enumerate(png_slots):
        input_label = f"{idx + 1}:v"
        is_last = idx == len(png_slots) - 1
        out_label = "" if is_last else f"[v{idx}]"
        parts.append(f"[{prev}][{input_label}]overlay=0:0:enable='between(t,{t_start},{t_end})'{out_label}")
        prev = f"v{idx}"

    filter_complex = ";".join(parts)
    cmd += [
        "-filter_complex",
        filter_complex,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "copy",
        out,
    ]
    return cmd


# ---------------------------------------------------------------------------
# Source dimensions helper
# ---------------------------------------------------------------------------


def _get_source_dimensions(source_path: str) -> tuple[int, int] | None:
    """Probe *source_path* with ffprobe and return ``(width, height)``.

    Returns:
        ``(width, height)`` tuple, or ``None`` on failure.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "stream=width,height",
                "-of",
                "csv=p=0",
                "-select_streams",
                "v:0",
                source_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split(",")
            if len(parts) >= 2:
                return int(parts[0]), int(parts[1])
    except Exception as exc:  # noqa: BLE001
        logger.warning("ffprobe dimension probe failed for %r: %s", source_path, exc)
    return None


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

_REQUIRED_OVERLAY_KEYS = ("tipo", "tiempo_inicio", "tiempo_fin", "titulo")


def _validate_source_selection(conf: dict) -> None:
    """Validate top-level required keys and the source XOR constraint.

    Raises:
        ValueError: On missing/invalid required keys or XOR source violation.
    """
    # Required top-level key: domain
    if "domain" not in conf:
        raise ValueError("Missing required conf key: 'domain'")

    # Required top-level key: overlays
    if "overlays" not in conf:
        raise ValueError("Missing required conf key: 'overlays'")

    overlays = conf["overlays"]
    if not overlays:
        raise ValueError("'overlays' must not be empty")

    # XOR source: exactly one of source_path OR (video_id + chapter_id)
    has_source_path = "source_path" in conf
    has_id_pair = "video_id" in conf and "chapter_id" in conf
    if has_source_path and has_id_pair:
        raise ValueError("Provide either 'source_path' OR ('video_id' + 'chapter_id'), not both.")
    if not has_source_path and not has_id_pair:
        raise ValueError("One of 'source_path' or ('video_id' + 'chapter_id') is required.")


def _validate_overlay(overlay: dict, index: int, tipos_cfg: dict) -> None:
    """Validate a single overlay dict: required keys, time ordering, tipo, fonts.

    Raises:
        ValueError: On missing required fields, invalid time ordering, or
            unknown tipo.
        FileNotFoundError: When the font file referenced by the tipo is absent.
    """
    # Required overlay keys
    for key in _REQUIRED_OVERLAY_KEYS:
        if key not in overlay:
            raise ValueError(f"Overlay[{index}] is missing required field: '{key}'")

    # Time ordering
    t_start = _parse_time(overlay["tiempo_inicio"])
    t_end = _parse_time(overlay["tiempo_fin"])
    if t_end <= t_start:
        raise ValueError(f"Overlay[{index}]: 'tiempo_fin' ({t_end}) must be greater than 'tiempo_inicio' ({t_start})")

    # Tipo must exist in domain config
    tipo = overlay["tipo"]
    if tipo not in tipos_cfg:
        raise ValueError(f"Overlay[{index}]: unknown tipo {tipo!r}. Available tipos: {list(tipos_cfg.keys())}")

    # Font file(s) must exist on disk; pillow tipos may have fontfile_sub too.
    for font_key in ("fontfile", "fontfile_sub"):
        font_path = tipos_cfg[tipo].get(font_key)
        if font_path and not os.path.exists(font_path):
            raise FileNotFoundError(f"Font file for tipo {tipo!r} ({font_key}) not found: {font_path}")


def validate_editor_input(conf: dict) -> None:
    """Validate the DAG run conf dict for the video editor.

    Checks:
        - ``domain`` key is present.
        - ``overlays`` key is present and non-empty.
        - Exactly one of ``source_path`` OR (``video_id`` + ``chapter_id``).
        - Each overlay has all required keys and valid time ordering.
        - The ``tipo`` in each overlay exists in the domain config.
        - The ``fontfile`` for every referenced ``tipo`` exists on disk.

    Args:
        conf: DAG run conf dict.

    Raises:
        ValueError: On missing/invalid required keys or XOR source violation.
        ConfigError: On unknown domain or unknown tipo.
        FileNotFoundError: When the font file referenced by a tipo is absent.
    """
    _validate_source_selection(conf)

    # Domain config lookup — raises ConfigError for unknown domain
    domain_cfg = get_domain_config(conf["domain"])
    tipos_cfg = domain_cfg["tipos"]

    for i, overlay in enumerate(conf["overlays"]):
        _validate_overlay(overlay, i, tipos_cfg)


# ---------------------------------------------------------------------------
# ffprobe duration helper
# ---------------------------------------------------------------------------


def _get_source_duration(source_path: str) -> float | None:
    """Probe *source_path* with ffprobe and return the duration in seconds.

    Args:
        source_path: Absolute path to the source video.

    Returns:
        Duration as float, or ``None`` if ffprobe fails or the value cannot
        be parsed (benign degradation — caller warns and skips clamping).
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                source_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return float(result.stdout.strip())
    except Exception as exc:  # noqa: BLE001
        logger.warning("ffprobe duration probe failed for %r: %s", source_path, exc)
    return None


# ---------------------------------------------------------------------------
# Orchestrator: apply_overlays
# ---------------------------------------------------------------------------


def _warn_overlays_exceeding_duration(overlays: list[dict], duration: float | None, source_path: str) -> None:
    """Log a warning for each overlay whose end time exceeds the source duration.

    ``duration`` uses an identity check (``is not None``) rather than
    truthiness: a probed duration of ``0.0`` is a valid (if degenerate)
    value and must still gate the warning loop.

    Args:
        overlays: List of overlay dicts (tipo, tiempo_inicio, tiempo_fin, titulo, …).
        duration: Probed source duration in seconds, or ``None`` if the probe failed.
        source_path: Absolute path to the source video (for the warning message).
    """
    if duration is not None:
        for overlay in overlays:
            t_end = _parse_time(overlay["tiempo_fin"])
            if t_end > duration:
                logger.warning(
                    "Overlay tiempo_fin=%.1f exceeds source duration=%.1f for %r.",
                    t_end,
                    duration,
                    source_path,
                )


def _run_drawtext_overlay(
    source_path: str,
    output_path: str,
    overlays: list[dict],
    domain_cfg: dict,
    timeout: int,
) -> dict:
    """Render *overlays* onto *source_path* via the ffmpeg drawtext filter.

    Args:
        source_path: Absolute path to the source video.
        output_path: Absolute path where the edited video will be written.
        overlays: List of overlay dicts (tipo, tiempo_inicio, tiempo_fin, titulo, …).
        domain_cfg: Domain config dict (from :func:`get_domain_config`).
        timeout: ffmpeg subprocess timeout in seconds.

    Returns:
        ``{"success": True, "output_path": output_path}``

    Raises:
        RuntimeError: When ffmpeg exits with a non-zero return code.
    """
    filter_str = build_drawtext_filter(overlays, domain_cfg)
    cmd = build_ffmpeg_drawtext_cmd(source_path, output_path, filter_str)
    logger.info("Running ffmpeg drawtext (timeout=%ds): %s", timeout, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed (rc={result.returncode}): {result.stderr}")
    logger.info("drawtext overlay applied: %r → %r", source_path, output_path)
    return {"success": True, "output_path": output_path}


def _run_pillow_overlay(
    source_path: str,
    output_path: str,
    overlays: list[dict],
    domain_cfg: dict,
    timeout: int,
) -> dict:
    """Render *overlays* as PNGs and composite them via ffmpeg filter_complex.

    ``dims`` uses an identity check (``is None``) rather than truthiness:
    a probed ``(0, 0)`` would still be a "present" value, distinct from a
    failed probe. Temp PNG files are tracked in a function-local list and
    always cleaned up via ``finally``, even when ffmpeg raises.

    Args:
        source_path: Absolute path to the source video.
        output_path: Absolute path where the edited video will be written.
        overlays: List of overlay dicts (tipo, tiempo_inicio, tiempo_fin, titulo, …).
        domain_cfg: Domain config dict (from :func:`get_domain_config`).
        timeout: ffmpeg subprocess timeout in seconds.

    Returns:
        ``{"success": True, "output_path": output_path}``

    Raises:
        RuntimeError: When ffmpeg exits with a non-zero return code.
    """
    dims = _get_source_dimensions(source_path)
    W, H = dims if dims is not None else (1280, 720)
    if dims is None:
        logger.warning("Could not probe video dimensions; defaulting to %dx%d.", W, H)

    tmp_files: list[str] = []
    try:
        png_slots: list[tuple[str, float, float]] = []
        for overlay in overlays:
            img = render_pillow_overlay(overlay, domain_cfg, W, H)
            fd, tmp_path = tempfile.mkstemp(suffix=".png")
            os.close(fd)
            img.save(tmp_path)
            tmp_files.append(tmp_path)
            t_start = _parse_time(overlay["tiempo_inicio"])
            t_end = _parse_time(overlay["tiempo_fin"])
            png_slots.append((tmp_path, t_start, t_end))

        cmd = build_ffmpeg_pillow_cmd(source_path, output_path, png_slots)
        logger.info("Running ffmpeg pillow composite (timeout=%ds): %s", timeout, " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg failed (rc={result.returncode}): {result.stderr}")
    finally:
        for tmp in tmp_files:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    logger.info("Pillow overlay applied: %r → %r", source_path, output_path)
    return {"success": True, "output_path": output_path}


def apply_overlays(
    source_path: str,
    output_path: str,
    overlays: list[dict],
    domain_cfg: dict,
) -> dict:
    """Burn overlays into *source_path* and write the result to *output_path*.

    Routes to the drawtext or Pillow renderer based on the ``renderer`` key
    in each overlay's tipo config. All overlays in a single call must use the
    same renderer; mixing raises ``ValueError``.

    Orchestration sequence:
        1. Probe source duration for timeout and clamp warnings.
        2. Determine renderer from tipo config (``"drawtext"`` or ``"pillow"``).
        3a. drawtext: build filter string → ffmpeg -vf.
        3b. pillow: render PNGs to temp files → ffmpeg filter_complex overlay.
        4. Run ffmpeg; raise ``RuntimeError`` on non-zero exit.
        5. Clean up temp PNG files (pillow path only).
        6. Return success dict.

    Args:
        source_path: Absolute path to the source video.
        output_path: Absolute path where the edited video will be written.
        overlays: List of overlay dicts (tipo, tiempo_inicio, tiempo_fin, titulo, …).
        domain_cfg: Domain config dict (from :func:`get_domain_config`).

    Returns:
        ``{"success": True, "output_path": output_path}``

    Raises:
        ValueError: When overlays mix ``"drawtext"`` and ``"pillow"`` renderers.
        RuntimeError: When ffmpeg exits with a non-zero return code.
    """
    tipos_cfg = domain_cfg["tipos"]

    # Determine renderers and enforce homogeneity.
    renderers = {tipos_cfg[o["tipo"]].get("renderer", "drawtext") for o in overlays}
    if len(renderers) > 1:
        raise ValueError(
            "All overlays in a single apply_overlays call must use the same renderer. "
            f"Found: {renderers}. Split drawtext and pillow overlays into separate calls."
        )
    renderer = renderers.pop()

    # 1. Probe duration.
    duration = _get_source_duration(source_path)

    # Warn if any overlay exceeds source duration.
    _warn_overlays_exceeding_duration(overlays, duration, source_path)

    timeout = compute_ffmpeg_timeout(duration if duration is not None else 0)

    if renderer == "drawtext":
        return _run_drawtext_overlay(source_path, output_path, overlays, domain_cfg, timeout)

    # Pillow path: render PNGs, composite via filter_complex.
    return _run_pillow_overlay(source_path, output_path, overlays, domain_cfg, timeout)
