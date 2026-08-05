"""Video overlay editor module for generic drawtext-based video editing.

Provides pure builder functions for constructing ffmpeg drawtext commands,
plus orchestration functions for source resolution, input validation, and
running ffmpeg via subprocess.

Public API::

    _escape_drawtext(text)           — escape ffmpeg drawtext metacharacters
    _parse_time(v)                   — normalise seconds value or SRT string
    build_drawtext_filter(overlays, domain_cfg) → str
    build_ffmpeg_drawtext_cmd(src, out, filter_str) → list[str]
    _resolve_source_path(conf)       → str
    _default_output_path(source_path) → str
    validate_editor_input(conf)
    apply_overlays(source_path, output_path, overlays, domain_cfg) → dict
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from congress_videos.config.paths import PROJECT_DATA_DIR
from congress_videos.config.video_editor_config import ConfigError, get_domain_config
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
            raise KeyError(
                f"Unknown tipo {tipo!r}. "
                f"Available tipos: {list(tipos_cfg.keys())}"
            )
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
        "-i", src,
        "-vf", filter_str,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "20",
        "-c:a", "copy",
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
# Input validation
# ---------------------------------------------------------------------------

_REQUIRED_OVERLAY_KEYS = ("tipo", "tiempo_inicio", "tiempo_fin", "titulo")


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
        raise ValueError(
            "Provide either 'source_path' OR ('video_id' + 'chapter_id'), not both."
        )
    if not has_source_path and not has_id_pair:
        raise ValueError(
            "One of 'source_path' or ('video_id' + 'chapter_id') is required."
        )

    # Domain config lookup — raises ConfigError for unknown domain
    domain_cfg = get_domain_config(conf["domain"])
    tipos_cfg = domain_cfg["tipos"]

    for i, overlay in enumerate(overlays):
        # Required overlay keys
        for key in _REQUIRED_OVERLAY_KEYS:
            if key not in overlay:
                raise ValueError(
                    f"Overlay[{i}] is missing required field: '{key}'"
                )

        # Time ordering
        t_start = _parse_time(overlay["tiempo_inicio"])
        t_end = _parse_time(overlay["tiempo_fin"])
        if t_end <= t_start:
            raise ValueError(
                f"Overlay[{i}]: 'tiempo_fin' ({t_end}) must be greater than "
                f"'tiempo_inicio' ({t_start})"
            )

        # Tipo must exist in domain config
        tipo = overlay["tipo"]
        if tipo not in tipos_cfg:
            raise ValueError(
                f"Overlay[{i}]: unknown tipo {tipo!r}. "
                f"Available tipos: {list(tipos_cfg.keys())}"
            )

        # Font file must exist on disk
        font_path = tipos_cfg[tipo]["fontfile"]
        if not os.path.exists(font_path):
            raise FileNotFoundError(
                f"Font file for tipo {tipo!r} not found: {font_path}"
            )


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
                "-v", "quiet",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
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


def apply_overlays(
    source_path: str,
    output_path: str,
    overlays: list[dict],
    domain_cfg: dict,
) -> dict:
    """Burn drawtext overlays into *source_path* and write the result to *output_path*.

    Orchestration sequence:
        1. Probe source duration via ffprobe.
        2. Warn (do not raise) if any overlay's ``tiempo_fin`` exceeds duration.
        3. Build the drawtext filter string.
        4. Build the ffmpeg argv list.
        5. Invoke ffmpeg via ``subprocess.run`` with an adaptive timeout.
        6. Raise ``RuntimeError`` on non-zero ffmpeg exit.
        7. Return success dict.

    Args:
        source_path: Absolute path to the source video.
        output_path: Absolute path where the edited video will be written.
        overlays: List of overlay dicts (tipo, tiempo_inicio, tiempo_fin, titulo, …).
        domain_cfg: Domain config dict (from :func:`get_domain_config`).

    Returns:
        ``{"success": True, "output_path": output_path}``

    Raises:
        RuntimeError: When ffmpeg exits with a non-zero return code.
    """
    # 1. Probe duration for timeout calculation and clamp warnings.
    duration = _get_source_duration(source_path)

    # 2. Warn if any overlay extends beyond the known source duration.
    if duration is not None:
        for overlay in overlays:
            t_end = _parse_time(overlay["tiempo_fin"])
            if t_end > duration:
                logger.warning(
                    "Overlay tiempo_fin=%.1f exceeds source duration=%.1f for %r. "
                    "ffmpeg will clamp naturally; no explicit clamping applied.",
                    t_end,
                    duration,
                    source_path,
                )

    # 3. Build the filter string.
    filter_str = build_drawtext_filter(overlays, domain_cfg)

    # 4. Build the ffmpeg argv list.
    cmd = build_ffmpeg_drawtext_cmd(source_path, output_path, filter_str)

    # 5. Run ffmpeg.
    timeout = compute_ffmpeg_timeout(duration if duration is not None else 0)
    logger.info("Running ffmpeg drawtext command (timeout=%ds): %s", timeout, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    # 6. Raise on failure.
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed with return code {result.returncode}: {result.stderr}"
        )

    logger.info("Video overlay applied successfully: %r → %r", source_path, output_path)
    return {"success": True, "output_path": output_path}
