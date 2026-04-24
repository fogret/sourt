import asyncio
import json
import subprocess


def _parse_probe_data(data: dict) -> dict | None:
    """
    Parse ffprobe JSON dict and return metadata dict with keys:
    video_codec, audio_codec, resolution, fps
    """
    if not data:
        return None
    streams = data.get('streams', [])
    video = None
    audio = None
    for s in streams:
        if s.get('codec_type') == 'video' and video is None:
            video = s
        elif s.get('codec_type') == 'audio' and audio is None:
            audio = s

    def _safe_get(d, key, default=None):
        if not d:
            return default
        return d.get(key, default)

    def _parse_rate(rate_str):
        try:
            if not rate_str:
                return None
            if isinstance(rate_str, (int, float)):
                return float(rate_str)
            if '/' in rate_str:
                num, den = rate_str.split('/')
                return float(num) / float(den) if float(den) != 0 else None
            return float(rate_str)
        except Exception:
            return None

    frame_rate_val = _parse_rate(_safe_get(video, 'avg_frame_rate') or _safe_get(video, 'r_frame_rate'))

    res = None
    try:
        w = _safe_get(video, 'width')
        h = _safe_get(video, 'height')
        if w and h:
            res = f"{w}x{h}"
    except Exception:
        res = None

    meta = {
        'video_codec': _safe_get(video, 'codec_name'),
        'audio_codec': _safe_get(audio, 'codec_name'),
        'resolution': res,
        'fps': frame_rate_val,
    }
    return meta


async def _safe_terminate(proc: asyncio.subprocess.Process | None):
    """统一安全销毁子进程，消除重复代码"""
    if not proc or proc.returncode is not None:
        return
    try:
        proc.kill()
    except Exception:
        pass
    try:
        await proc.wait()
    except Exception:
        pass


async def probe_url(url: str, headers: dict = None, timeout: int = 10) -> dict | None:
    """
    Use ffprobe to get metadata for the first video and audio streams.
    Returns a dict with keys: video_codec, audio_codec, resolution, fps
    """
    proc = None
    try:
        header_str = ''.join(f'{k}: {v}\r\n' for k, v in (headers or {}).items()) if headers else ''
        args = [
            'ffprobe', '-v', 'error',
            '-show_format', '-show_streams',
            '-print_format', 'json'
        ]
        if header_str:
            args += ['-headers', header_str]
        args.append(url)

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)

        if out:
            data = json.loads(out.decode('utf-8', errors='ignore'))
            return _parse_probe_data(data)
    except (asyncio.TimeoutError, Exception):
        pass
    finally:
        await _safe_terminate(proc)
    return None


def probe_url_sync(url: str, headers: dict = None, timeout: int = 10) -> dict | None:
    """
    Synchronous wrapper around ffprobe to obtain metadata for the first video and audio streams.
    """
    header_str = ''.join(f'{k}: {v}\r\n' for k, v in (headers or {}).items()) if headers else ''
    args = [
        'ffprobe', '-v', 'error',
        '-show_format', '-show_streams',
        '-print_format', 'json'
    ]
    if header_str:
        args += ['-headers', header_str]
    args.append(url)

    try:
        res = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=timeout
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return None

    candidate = res.stdout.strip() or res.stderr.strip()
    if not candidate:
        return None

    try:
        data = json.loads(candidate)
    except json.JSONDecodeError:
        idx = candidate.find('{')
        if idx == -1:
            return None
        try:
            data = json.loads(candidate[idx:])
        except Exception:
            return None
    except Exception:
        return None
    return _parse_probe_data(data)


async def get_resolution_ffprobe(url: str, headers: dict = None, timeout: int = 10) -> str | None:
    """
    Use ffprobe to get width and height of the first video stream and return as 'WIDTHxHEIGHT'.
    """
    proc = None
    try:
        header_str = ''.join(f'{k}: {v}\r\n' for k, v in (headers or {}).items()) if headers else ''
        args = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-print_format', 'json'
        ]
        if header_str:
            args += ['-headers', header_str]
        args.append(url)

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)

        if out:
            j = json.loads(out.decode('utf-8', errors='ignore'))
            streams = j.get('streams') or []
            if streams:
                s = streams[0]
                w, h = s.get('width'), s.get('height')
                if w and h:
                    return f"{w}x{h}"
    except (asyncio.TimeoutError, Exception):
        pass
    finally:
        await _safe_terminate(proc)
    return None
