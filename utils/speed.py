import asyncio
import http.cookies
import re
from time import time
from urllib.parse import quote, urljoin

import m3u8
from aiohttp import ClientSession, TCPConnector

import utils.constants as constants
from utils.config import config
from utils.ffmpeg import probe_url, ffmpeg_url
from utils.i18n import t
from utils.requests.tools import headers as request_headers
from utils.tools import get_resolution_value
from utils.types import TestResult, ChannelTestResult, TestResultCacheData

http.cookies._is_legal_key = lambda _: True
cache: TestResultCacheData = {}
speed_test_timeout = config.speed_test_timeout
speed_test_filter_host = config.speed_test_filter_host
open_filter_resolution = config.open_filter_resolution
min_resolution_value = config.min_resolution_value
max_resolution_value = config.max_resolution_value
open_supply = config.open_supply
open_filter_speed = config.open_filter_speed
min_speed_value = config.min_speed
resolution_speed_map = config.resolution_speed_map
speed_test_limit = config.speed_test_limit
m3u8_headers = ['application/x-mpegurl', 'application/vnd.apple.mpegurl', 'audio/mpegurl', 'audio/x-mpegurl']
default_ipv6_delay = 0.1
default_ipv6_resolution = "1920x1080"
default_ipv6_result = {
    'speed': float("inf"),
    'delay': default_ipv6_delay,
    'resolution': default_ipv6_resolution
}

min_measure_time = 1.0
stability_window = 4
stability_threshold = 0.12


async def get_speed_with_download(url: str, headers: dict = None, session: ClientSession = None,
                                  timeout: int = speed_test_timeout) -> dict[str, float | None]:
    """
    Get the speed of the url with a total timeout
    """
    start_time = time()
    delay = -1
    total_size = 0
    min_bytes = 64 * 1024
    last_sample_time = start_time
    last_sample_size = 0

    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False

    speed_samples: list[float] = []
    try:
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status != 200:
                raise Exception("Invalid response")
            delay = int(round((time() - start_time) * 1000))
            async for chunk in response.content.iter_any():
                if chunk:
                    total_size += len(chunk)
                    now = time()
                    elapsed = now - start_time
                    delta_t = now - last_sample_time
                    delta_b = total_size - last_sample_size
                    if delta_t > 0 and delta_b > 0:
                        inst_speed = delta_b / delta_t / 1024.0 / 1024.0
                        speed_samples.append(inst_speed)
                        last_sample_time = now
                        last_sample_size = total_size
                    if (elapsed >= min_measure_time and total_size >= min_bytes
                            and len(speed_samples) >= stability_window):
                        window = speed_samples[-stability_window:]
                        mean = sum(window) / len(window)
                        if mean > 0 and (max(window) - min(window)) / mean < stability_threshold:
                            total_time = elapsed
                            return {
                                'speed': total_size / total_time / 1024 / 1024,
                                'delay': delay,
                                'size': total_size,
                                'time': total_time,
                            }
    except:
        pass
    finally:
        total_time = time() - start_time
        if created_session:
            await session.close()
        speed_value = total_size / total_time / 1024 / 1024 if total_time > 0 else 0.0
        return {
            'speed': speed_value,
            'delay': delay,
            'size': total_size,
            'time': total_time,
        }


async def get_headers(url: str, headers: dict = None, session: ClientSession = None, timeout: int = 3) -> dict:
    """
    Get the headers of the url
    """
    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False
    res_headers = {}
    try:
        async with session.head(url, headers=headers, timeout=timeout) as response:
            res_headers = response.headers
    except:
        pass
    finally:
        if created_session:
            await session.close()
    return res_headers


async def get_url_content(url: str, headers: dict = None, session: ClientSession = None,
                          timeout: int = speed_test_timeout) -> str:
    """
    Get the content of the url
    """
    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False
    content = ""
    try:
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status == 200:
                content = await response.text()
            else:
                raise Exception("Invalid response")
    except:
        pass
    finally:
        if created_session:
            await session.close()
    return content


def check_m3u8_valid(headers: dict) -> bool:
    """
    Check if the m3u8 url is valid
    """
    content_type = headers.get('Content-Type', '').lower()
    if not content_type:
        return False
    return any(item in content_type for item in m3u8_headers)


def _parse_time_to_seconds(t: str) -> float:
    """
    Parse time string to seconds
    """
    if not t:
        return 0.0
    parts = [p.strip() for p in t.split(':') if p.strip() != ""]
    if not parts:
        return 0.0
    try:
        total = 0.0
        for i, part in enumerate(reversed(parts)):
            total += float(part) * (60 ** i)
        return total
    except Exception:
        return 0.0


async def get_result(url: str, headers: dict = None, resolution: str = None,
                     filter_resolution: bool = config.open_filter_resolution,
                     timeout: int = speed_test_timeout) -> dict[str, float | None]:
    """
    Get the test result of the url
    """
    info = {'speed': 0.0, 'delay': -1, 'resolution': resolution}
    location = None
    try:
        url = quote(url, safe=':/?$&=@[]%').partition('$')[0]
        async with ClientSession(connector=TCPConnector(ssl=False), trust_env=True) as session:
            res_headers = await get_headers(url, headers, session)
            if not res_headers:
                return info
            location = res_headers.get('Location')
            if location:
                info.update(await get_result(location, headers, resolution, filter_resolution, timeout))
            else:
                url_content = await get_url_content(url, headers, session, timeout)
                if url_content:
                    m3u8_obj = m3u8.loads(url_content)
                    playlists = m3u8_obj.playlists
                    segments = m3u8_obj.segments
                    if playlists:
                        best_playlist = max(m3u8_obj.playlists, key=lambda p: p.stream_info.bandwidth)
                        playlist_url = urljoin(url, best_playlist.uri)
                        playlist_content = await get_url_content(playlist_url, headers, session, timeout)
                        if playlist_content:
                            media_playlist = m3u8.loads(playlist_content)
                            segment_urls = [urljoin(playlist_url, segment.uri) for segment in media_playlist.segments]
                    else:
                        segment_urls = [urljoin(url, segment.uri) for segment in segments]
                    if not segment_urls:
                        raise Exception("Segment urls not found")
                else:
                    res_info = await get_speed_with_download(url, headers, session, timeout)
                    info.update({'speed': res_info['speed'], 'delay': res_info['delay']})
                start_time = time()
                sampled_segment_urls = sample_segment_urls(segment_urls, speed_test_limit)
                tasks = [get_speed_with_download(ts_url, headers, session, timeout) for ts_url in sampled_segment_urls]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                total_size = sum(result['size'] for result in results if isinstance(result, dict))
                total_time = sum(result['time'] for result in results if isinstance(result, dict))
                info['speed'] = total_size / total_time / 1024 / 1024 if total_time > 0 else 0
                info['delay'] = int(round((time() - start_time) * 1000))
                try:
                    if round(info['speed'], 2) == 0 and info['delay'] != -1:
                        ff_out = await ffmpeg_url(url, headers, timeout)
                        if ff_out:
                            try:
                                parsed = get_video_info(ff_out)
                                if parsed:
                                    parsed_speed = parsed.get('speed')
                                    parsed_resolution = parsed.get('resolution')
                                    parsed_fps = parsed.get('fps')
                                    parsed_video_codec = parsed.get('video_codec')
                                    parsed_audio_codec = parsed.get('audio_codec')
                                    if parsed_speed:
                                        info['speed'] = parsed_speed
                                    if parsed_resolution:
                                        info['resolution'] = parsed_resolution
                                    if parsed_fps:
                                        info['fps'] = parsed_fps
                                    if parsed_video_codec:
                                        info['video_codec'] = parsed_video_codec
                                    if parsed_audio_codec:
                                        info['audio_codec'] = parsed_audio_codec
                            except Exception:
                                pass
                except Exception:
                    pass
    except:
        pass
    finally:
        if filter_resolution and not location and not info.get('resolution') and info.get('delay') != -1:
            try:
                probed = await probe_url(url, headers, timeout=timeout)
                if probed:
                    info['resolution'] = probed.get('resolution')
                    info['fps'] = probed.get('fps')
                    info['video_codec'] = probed.get('video_codec')
                    info['audio_codec'] = probed.get('audio_codec')
            except Exception:
                pass
    return info


async def get_delay_requests(url, timeout=speed_test_timeout, proxy=None):
    """
    Get the delay of the url by requests
    """
    async with ClientSession(
            connector=TCPConnector(ssl=False), trust_env=True
    ) as session:
        start = time()
        end = None
        try:
            async with session.get(url, timeout=timeout, proxy=proxy) as response:
                if response.status == 404:
                    return -1
                content = await response.read()
                if content:
                    end = time()
                else:
                    return -1
        except Exception as e:
            return -1
        return int(round((end - start) * 1000)) if end else -1


def get_video_info(video_info):
    """
    Get the video info from ffmpeg stderr
    """
    resolution = None
    fps = None
    video_codec = None
    audio_codec = None
    if video_info is not None:
        match = re.search(r"(\d{3,4}x\d{3,4})", video_info)
        if match:
            resolution = match.group(0)
        m_fps = re.search(r"(\d+(?:\.\d+)?)\s*fps", video_info, re.IGNORECASE)
        if not m_fps:
            m_fps = re.search(r"(\d+(?:\.\d+)?)\s*tbr", video_info, re.IGNORECASE)
        if not m_fps:
            m_fps = re.search(r"(\d+(?:\.\d+)?)\s*tbn", video_info, re.IGNORECASE)
        if m_fps:
            try:
                fps = float(m_fps.group(1))
            except Exception:
                fps = None
        m_vc = re.search(r"Video:\s*([^,\n\r(]+)", video_info, re.IGNORECASE)
        if m_vc:
            vc = m_vc.group(1).strip()
            vc = vc.split(',')[0].split()[0]
            if vc:
                video_codec = vc
        m_ac = re.search(r"Audio:\s*([^,\n\r(]+)", video_info, re.IGNORECASE)
        if m_ac:
            ac = m_ac.group(1).strip()
            ac = ac.split(',')[0].split()[0]
            if ac:
                audio_codec = ac

    def parse_size_value(value_str, unit):
        try:
            val = float(value_str)
        except:
            return 0.0
        if not unit:
            return val
        ul = unit.lower()
        if ul in ("b", "bytes"):
            return val
        if ul in ("kib", "k"):
            return val * 1024
        if ul in ("kb",):
            return val * 1000
        if ul in ("mib", "mb"):
            return val * 1048576
        return val

    speed_val = None
    try:
        total_bytes = 0.0
        m_video = re.search(r"video:\s*([\d.]+)\s*(KiB|MiB|B|kB)?", video_info, re.I)
        m_audio = re.search(r"audio:\s*([\d.]+)\s*(KiB|MiB|B|kB)?", video_info, re.I)
        if m_video:
            total_bytes += parse_size_value(m_video.group(1), m_video.group(2))
        if m_audio:
            total_bytes += parse_size_value(m_audio.group(1), m_audio.group(2))
        m_time = re.search(r"time=\s*([\d:.]+)", video_info)
        if total_bytes > 0 and m_time:
            sec = _parse_time_to_seconds(m_time.group(1))
            if sec > 0:
                speed_val = total_bytes / sec / 1048576
    except:
        pass

    if not speed_val:
        try:
            m_lsize = re.search(r"Lsize=\s*([\d.]+)\s*(KiB|MiB|B|kB)?", video_info, re.I)
            m_size = re.search(r"size=\s*([\d.]+)\s*(KiB|MiB|B|kB)?", video_info, re.I)
            m_time = re.search(r"time=\s*([\d:.]+)", video_info)
            size_b = 0
            if m_lsize and m_lsize.group(1).upper() != "N/A":
                size_b = parse_size_value(m_lsize.group(1), m_lsize.group(2))
            elif m_size:
                size_b = parse_size_value(m_size.group(1), m_size.group(2))
            if size_b > 0 and m_time:
                sec = _parse_time_to_seconds(m_time.group(1))
                if sec > 0:
                    speed_val = size_b / sec / 1048576
        except:
            pass

    if not speed_val:
        try:
            m_bitrate = re.search(r"bitrate=\s*([\d.]+)\s*k?bits/s", video_info)
            if m_bitrate:
                kbps = float(m_bitrate.group(1))
                speed_val = kbps / 8 / 1024
        except:
            pass

    return {
        'resolution': resolution,
        'fps': fps,
        'video_codec': video_codec,
        'audio_codec': audio_codec,
        'speed': speed_val,
    }


def sample_segment_urls(segment_urls: list, limit: int) -> list:
    if not segment_urls:
        return []
    try:
        limit = int(limit)
    except:
        limit = 0
    total = len(segment_urls)
    if limit <= 0 or limit >= total:
        return list(segment_urls)
    if limit == 1:
        return [segment_urls[total // 2]]
    indices = []
    for i in range(limit):
        idx = round(i * (total - 1) / (limit - 1))
        indices.append(idx)
    seen = set()
    sampled = []
    for idx in indices:
        idx = max(0, min(idx, total - 1))
        if idx not in seen:
            seen.add(idx)
            sampled.append(segment_urls[idx])
    return sampled


def get_avg_result(result) -> TestResult:
    return {
        'speed': sum(item['speed'] or 0 for item in result) / len(result),
        'delay': max(int(sum(item['delay'] or -1 for item in result) / len(result)), -1),
        'resolution': max((item['resolution'] for item in result), key=get_resolution_value)
    }


def get_speed_result(key: str) -> TestResult:
    if key in cache:
        return get_avg_result(cache[key])
    else:
        return {'speed': 0, 'delay': -1, 'resolution': None}


async def get_speed(data, headers=None, ipv6_proxy=None, filter_resolution=open_filter_resolution,
                    timeout=speed_test_timeout, logger=None, callback=None) -> TestResult:
    url = data['url']
    resolution = data['resolution']
    result: TestResult = {'speed': 0, 'delay': -1, 'resolution': resolution}
    headers = {**request_headers, **(headers or {})}

    try:
        cache_key = data['host'] if speed_test_filter_host else url
        if cache_key and cache_key in cache:
            result = get_avg_result(cache[cache_key])
        else:
            if data['ipv_type'] == "ipv6" and ipv6_proxy:
                result.update(default_ipv6_result)
            elif (hasattr(constants, 'rt_url_pattern') and constants.rt_url_pattern.match(url)) or "/rtp/" in url:
                try:
                    start_time = time()
                    ff_out = await ffmpeg_url(url, headers, timeout)
                    parsed = get_video_info(ff_out) if ff_out else {}
                    result['delay'] = int(round((time() - start_time) * 1000))
                    result['speed'] = parsed.get('speed', 0.5)
                    result['resolution'] = parsed.get('resolution', '1280x720')
                    result['fps'] = parsed.get('fps')
                    result['video_codec'] = parsed.get('video_codec')
                    result['audio_codec'] = parsed.get('audio_codec')
                except Exception:
                    result['delay'] = 200
                    result['speed'] = 0.5
                    result['resolution'] = '1280x720'
            else:
                result.update(await get_result(url, headers, resolution, filter_resolution, timeout))
            if cache_key:
                cache.setdefault(cache_key, []).append(result)
    except Exception:
        pass
    finally:
        if callback:
            callback()
        if logger:
            origin = data.get('origin')
            origin_name = t(f"name.{origin}") if origin else origin
            logger.info(
                f"ID: {data.get('id')}, {t('name.name')}: {data.get('name')}, {t('pbar.url')}: {data.get('url')}, {t('name.from')}: {origin_name}, {t('name.ipv_type')}: {data.get('ipv_type')}, {t('name.location')}: {data.get('location')}, {t('name.isp')}: {data.get('isp')}, {t('name.delay')}: {result.get('delay') or -1} ms, {t('name.speed')}: {result.get('speed') or 0:.2f} M/s, {t('name.resolution')}: {result.get('resolution')}, {t('name.fps')}: {result.get('fps') or t('name.unknown')}, {t('name.video_codec')}: {result.get('video_codec') or t('name.unknown')}, {t('name.audio_codec')}: {result.get('audio_codec') or t('name.unknown')}"
            )
    return result


def get_sort_result(
        results,
        supply=open_supply,
        filter_speed=open_filter_speed,
        min_speed=min_speed_value,
        filter_resolution=open_filter_resolution,
        min_resolution=min_resolution_value,
        max_resolution=max_resolution_value,
        ipv6_support=True
) -> list[ChannelTestResult]:
    total_result = []
    for res in results:
        if not ipv6_support and res["ipv_type"] == "ipv6":
            res.update(default_ipv6_result)
        sp = res.get("speed", 0)
        dl = res.get("delay")
        reso = res.get("resolution")
        if dl == -1:
            continue
        if not supply:
            if filter_speed and sp < resolution_speed_map.get(reso, min_speed):
                continue
            if filter_resolution and reso:
                rv = get_resolution_value(reso)
                if rv < min_resolution or rv > max_resolution:
                    continue
        total_result.append(res)
    total_result.sort(key=lambda x: x.get("speed", 0), reverse=True)
    return total_result


def clear_cache():
    global cache
    cache = {}
