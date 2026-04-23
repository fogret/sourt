import asyncio
import gzip
import hashlib
import json
import math
import os
import pickle
import re
import tempfile
from collections import defaultdict, Counter, OrderedDict
from itertools import chain
from logging import INFO
from typing import cast

import utils.constants as constants
from utils.alias import Alias
from utils.config import config
from utils.db import ensure_result_data_schema
from utils.db import get_db_connection, return_db_connection
from utils.ffmpeg import check_ffmpeg_installed_status
from utils.frozen import is_url_frozen, mark_url_bad, mark_url_good
from utils.i18n import t
from utils.ip_checker import IPChecker
from utils.speed import (
    get_speed,
    get_speed_result,
    get_sort_result
)
from utils.tools import (
    format_name,
    get_name_value,
    check_url_by_keywords,
    get_total_urls,
    add_url_info,
    resource_path,
    get_name_urls_from_file,
    get_logger,
    get_datetime_now,
    get_url_host,
    check_ipv_type_match,
    convert_to_m3u,
    custom_print,
    get_name_uri_from_dir,
    get_resolution_value,
    get_public_url,
    build_path_list,
    get_real_path,
    count_files_by_ext,
    close_logger_handlers,
    fast_get_ipv_type
)
from utils.types import ChannelData, OriginType, CategoryChannelData, WhitelistMaps
from utils.whitelist import is_url_whitelisted, get_whitelist_url, get_whitelist_total_count

channel_alias = Alias()
ip_checker = IPChecker()
location_list = config.location
isp_list = config.isp
open_filter_speed = config.open_filter_speed
min_speed = config.min_speed
open_filter_resolution = config.open_filter_resolution
min_resolution_value = config.min_resolution_value
resolution_speed_map = config.resolution_speed_map
open_history = config.open_history
open_local = config.open_local
open_rtmp = config.open_rtmp
retain_origin = ["whitelist", "hls"]

_TOTAL_URLS_CACHE_MAX_SIZE = 2048
_TOTAL_URLS_CACHE = OrderedDict()


def _build_total_urls_signature(info_list: list[ChannelData]) -> str:
    """
    Build a stable signature for a channel info list.
    """
    hasher = hashlib.sha1()
    for info in info_list or []:
        if not isinstance(info, dict):
            hasher.update(repr(info).encode("utf-8", errors="ignore"))
            hasher.update(b"\x1e")
            continue

        origin = info.get("origin") or ""
        extra_info = info.get("extra_info") or ""
        if origin not in retain_origin and not extra_info:
            extra_info = constants.origin_map.get(origin, "")

        hasher.update(
            "\x1f".join((
                str(info.get("id", "")),
                info.get("url") or "",
                origin,
                info.get("ipv_type") or "",
                extra_info,
            )).encode("utf-8", errors="ignore")
        )
        hasher.update(b"\x1e")

    return hasher.hexdigest()


def _get_total_urls_cached(
        info_list: list[ChannelData],
        ipv_type_prefer,
        origin_type_prefer,
        rtmp_type=None,
        apply_limit: bool = True,
) -> tuple:
    """
    Cached wrapper for `get_total_urls()`.
    """
    ipv_key = tuple(ipv_type_prefer or ())
    origin_key = tuple(origin_type_prefer or ())
    rtmp_key = tuple(rtmp_type or ())
    cache_key = (
        _build_total_urls_signature(info_list),
        ipv_key,
        origin_key,
        rtmp_key,
        bool(apply_limit),
        config.urls_limit,
    )
    cached = _TOTAL_URLS_CACHE.get(cache_key)
    if cached is not None:
        _TOTAL_URLS_CACHE.move_to_end(cache_key)
        return cached

    total_urls = tuple(get_total_urls(info_list, ipv_type_prefer, origin_type_prefer, rtmp_type, apply_limit))
    _TOTAL_URLS_CACHE[cache_key] = total_urls
    if len(_TOTAL_URLS_CACHE) > _TOTAL_URLS_CACHE_MAX_SIZE:
        _TOTAL_URLS_CACHE.popitem(last=False)
    return total_urls


def format_channel_data(url: str, origin: OriginType) -> ChannelData:
    """
    Format the channel data
    """
    url_partition = url.partition("$")
    url = url_partition[0]
    info = url_partition[2]
    if info and info.startswith("!"):
        origin = "whitelist"
        info = info[1:]
    return {
        "id": hash(url),
        "url": url,
        "host": get_url_host(url),
        "origin": cast(OriginType, origin),
        "ipv_type": None,
        "extra_info": info
    }


def check_channel_need_frozen(info) -> bool:
    """
    Check if the channel need to be frozen
    """
    delay = info.get("delay", 0)
    if delay == -1 or info.get("speed", 0) == 0:
        return True
    if info.get("resolution"):
        if get_resolution_value(info["resolution"]) < min_resolution_value:
            return True
    return False


def get_channel_data_from_file(channels, file, whitelist_maps, blacklist,
                               local_data=None, hls_data=None) -> CategoryChannelData:
    """
    Get the channel data from the file
    """
    current_category = ""
    matched_local_names = set()
    matched_hls_names = set()
    unmatch_category = t("content.unmatch_channel")

    def append_unmatch_data(name: str, info_list: list):
        category_dict = channels[unmatch_category]
        if name not in category_dict:
            category_dict[name] = []
        existing_urls = {d.get("url") for d in category_dict.get(name, []) if d.get("url")}
        for item in info_list:
            if not item:
                continue
            url = item.get("url")
            if not url or url in existing_urls:
                continue
            category_dict[name].append(item)
            existing_urls.add(url)

    for line in file:
        line = line.strip()
        if "#genre#" in line:
            current_category = re.split(r"[，,]", line, maxsplit=1)[0]
        else:
            name_value = get_name_value(
                line, pattern=constants.demo_txt_pattern, check_value=False
            )
            if name_value and name_value[0]:
                name = name_value[0]["name"]
                url = name_value[0]["value"]
                category_dict = channels[current_category]
                first_time = name not in category_dict
                if first_time:
                    category_dict[name] = []
                existing_urls = {d.get("url") for d in category_dict.get(name, []) if d.get("url")}

                if first_time:
                    for whitelist_url in get_whitelist_url(whitelist_maps, name):
                        formatted = format_channel_data(whitelist_url, "whitelist")
                        if formatted["url"] not in existing_urls:
                            category_dict[name].append(formatted)
                            existing_urls.add(formatted["url"])

                    if hls_data and name in hls_data:
                        matched_hls_names.add(name)
                        for hls_url in hls_data[name]:
                            formatted = format_channel_data(hls_url, "hls")
                            if formatted["url"] not in existing_urls:
                                category_dict[name].append(formatted)
                                existing_urls.add(formatted["url"])

                    if open_local and local_data:
                        alias_names = channel_alias.get(name)
                        alias_names.update([name, format_name(name)])
                        for alias_name in alias_names:
                            if alias_name in local_data:
                                matched_local_names.add(alias_name)
                                for local_url in local_data[alias_name]:
                                    if not check_url_by_keywords(local_url, blacklist):
                                        local_url_origin: OriginType = "whitelist" if is_url_whitelisted(whitelist_maps,
                                                                                                         local_url,
                                                                                                         name) else "local"
                                        formatted = format_channel_data(local_url, local_url_origin)
                                        if formatted["url"] not in existing_urls:
                                            category_dict[name].append(formatted)
                                            existing_urls.add(formatted["url"])
                            elif alias_name.startswith("re:"):
                                raw_pattern = alias_name[3:]
                                try:
                                    pattern = re.compile(raw_pattern)
                                    for local_name in local_data:
                                        if re.match(pattern, local_name):
                                            matched_local_names.add(local_name)
                                            for local_url in local_data[local_name]:
                                                if not check_url_by_keywords(local_url, blacklist):
                                                    local_url_origin: OriginType = "whitelist" if is_url_whitelisted(
                                                        whitelist_maps,
                                                        local_url,
                                                        name) else "local"
                                                    formatted = format_channel_data(local_url, local_url_origin)
                                                    if formatted["url"] not in existing_urls:
                                                        category_dict[name].append(formatted)
                                                        existing_urls.add(formatted["url"])
                                except re.error:
                                    pass
                if url:
                    if is_url_whitelisted(whitelist_maps, url, name):
                        formatted = format_channel_data(url, "whitelist")
                        if formatted["url"] not in existing_urls:
                            category_dict[name].append(formatted)
                            existing_urls.add(formatted["url"])
                    elif open_local and not check_url_by_keywords(url, blacklist):
                        formatted = format_channel_data(url, "local")
                        if formatted["url"] not in existing_urls:
                            category_dict[name].append(formatted)
                            existing_urls.add(formatted["url"])

    if config.open_unmatch_category:
        if open_local and local_data:
            for local_name, local_urls in local_data.items():
                if local_name in matched_local_names:
                    continue
                unmatch_local_urls = [
                    format_channel_data(local_url, "whitelist" if is_url_whitelisted(whitelist_maps, local_url,
                                                                                     local_name) else "local")
                    for local_url in local_urls
                    if not check_url_by_keywords(local_url, blacklist)
                ]
                if unmatch_local_urls:
                    append_unmatch_data(local_name, unmatch_local_urls)

        if hls_data and open_rtmp:
            for hls_name, hls_urls in hls_data.items():
                if hls_name in matched_hls_names:
                    continue
                unmatch_hls_urls = [format_channel_data(hls_url, "hls") for hls_url in hls_urls]
                if unmatch_hls_urls:
                    append_unmatch_data(hls_name, unmatch_hls_urls)
    return channels


def get_channel_items(whitelist_maps, blacklist) -> CategoryChannelData:
    """
    Get the channel items from the source file
    """
    user_source_file = resource_path(config.source_file)
    channels = defaultdict(lambda: defaultdict(list))
    hls_data = None
    if config.open_rtmp:
        hls_data = get_name_uri_from_dir(constants.hls_path)
    local_paths = build_path_list(constants.local_dir_path)
    local_data = get_name_urls_from_file([get_real_path(constants.local_path)] + local_paths)
    whitelist_count = get_whitelist_total_count(whitelist_maps)
    blacklist_count = len(blacklist)
    channel_logo_count = count_files_by_ext(resource_path(constants.channel_logo_path), [config.logo_type])
    if whitelist_count:
        print(t("msg.whitelist_found").format(count=whitelist_count))
    if blacklist_count:
        print(t("msg.blacklist_found").format(count=blacklist_count))
    if channel_logo_count:
        print(t("msg.channel_logo_found").format(count=channel_logo_count))

    if os.path.exists(user_source_file):
        with open(user_source_file, "utf-8") as file:
            channels = get_channel_data_from_file(
                channels, file, whitelist_maps, blacklist, local_data, hls_data
            )

    source_name_targets = defaultdict(list)
    for cate, data in channels.items():
        for name in data.keys():
            source_name_targets[format_channel_name(name)].append((cate, name))

    if config.open_history and os.path.exists(constants.cache_path):
        unmatched_history = defaultdict(list)

        def _append_history_items(channel_data, info_list):
            urls = [url for item in channel_data if (url := item.get("url"))]
            for info in info_list:
                if not info:
                    continue
                info_url = info.get("url")
                try:
                    if info.get("origin") in retain_origin or check_url_by_keywords(info_url, blacklist):
                        continue
                    if check_channel_need_frozen(info):
                        mark_url_bad(info_url, initial=True)
                        continue
                except Exception:
                    pass
                if info_url and info_url not in urls:
                    channel_data.append(info)
                    urls.append(info_url)

        try:
            with gzip.open(constants.cache_path, "rb") as file:
                old_result = pickle.load(file) or {}
                for cate, data in old_result.items():
                    for name, info_list in data.items():
                        targets = source_name_targets.get(format_channel_name(name))
                        if targets:
                            for target_cate, target_name in targets:
                                channel_data = channels[target_cate][target_name]
                                _append_history_items(channel_data, info_list)
                                if not channel_data:
                                    for info in info_list:
                                        old_result_url = info.get("url") if info else None
                                        if info and info.get("origin") not in retain_origin and old_result_url and not check_url_by_keywords(old_result_url, blacklist):
                                            channel_data.append(info)
                        else:
                            unmatched_history[name].extend(info_list)
        except Exception as e:
            print(t("msg.error_load_cache").format(info=e))

        if unmatched_history and config.open_unmatch_category:
            unmatch_category = t("content.unmatch_channel")
            for name, info_list in unmatched_history.items():
                append_data_to_info_data(
                    channels,
                    unmatch_category,
                    name,
                    info_list,
                    whitelist_maps=whitelist_maps,
                    blacklist=blacklist,
                    skip_validation=True,
                )
    return channels


def format_channel_name(name):
    """
    Format the channel name with sub and replace and lower
    """
    return channel_alias.get_primary(name)


def channel_name_is_equal(name1, name2):
    """
    Check if the channel name is equal
    """
    name1_format = format_channel_name(name1)
    name2_format = format_channel_name(name2)
    return name1_format == name2_format


def get_channel_results_by_name(name, data):
    """
    Get channel results from data by name
    """
    format_name = format_channel_name(name)
    results = data.get(format_name, [])
    return results


def get_channel_url(text):
    """
    Get the url from text
    """
    # 放开所有链接格式，不再限制协议
    if not text:
        return None
    return text.strip()


def init_info_data(data: dict, category: str, name: str) -> None:
    """
    Initialize channel info data structure if not exists
    """
    data.setdefault(category, {}).setdefault(name, [])


def append_data_to_info_data(
        info_data: dict,
        category: str,
        name: str,
        data: list,
        origin: str = None,
        whitelist_maps: WhitelistMaps = None,
        blacklist: list = None,
        ipv_type_data: dict = None,
        skip_validation: bool = False
) -> None:
    """
    Append channel data to total info data with deduplication and validation
    """
    init_info_data(info_data, category, name)

    channel_list = info_data[category][name]
    existing_map = {info["url"]: idx for idx, info in enumerate(channel_list) if "url" in info}

    for item in data:
        try:
            channel_id = item.get("id") or hash(item["url"])
            raw_url = item.get("url")
            host = item.get("host") or (get_url_host(raw_url) if raw_url else None)
            date = item.get("date")
            delay = item.get("delay")
            speed = item.get("speed")
            resolution = item.get("resolution")
            url_origin = item.get("origin", origin)
            ipv_type = item.get("ipv_type")
            location = item.get("location")
            isp = item.get("isp")
            headers = item.get("headers")
            catchup = item.get("catchup")
            extra_info = item.get("extra_info", "")

            if not raw_url:
                continue

            normalized_url = raw_url
            if url_origin not in retain_origin:
                normalized_url = get_channel_url(raw_url)
                if not normalized_url:
                    continue
                if is_url_frozen(normalized_url):
                    continue
                if blacklist and check_url_by_keywords(normalized_url, blacklist):
                    continue

            if url_origin != "whitelist" and whitelist_maps and is_url_whitelisted(whitelist_maps, normalized_url, name):
                url_origin = "whitelist"

            if skip_validation and url_origin not in retain_origin and not ipv_type:
                if ipv_type_data and host in ipv_type_data:
                    ipv_type = ipv_type_data[host]
                else:
                    ipv_type = fast_get_ipv_type(host)
                    if ipv_type_data is not None and host:
                        ipv_type_data[host] = ipv_type

            if normalized_url in existing_map:
                existing_idx = existing_map[normalized_url]
                existing_origin = channel_list[existing_idx].get("origin")
                if existing_origin != "whitelist" and url_origin == "whitelist":
                    channel_list[existing_idx] = {
                        "id": channel_id,
                        "url": normalized_url,
                        "host": host or get_url_host(normalized_url),
                        "date": date,
                        "delay": delay,
                        "speed": speed,
                        "resolution": resolution,
                        "origin": url_origin,
                        "ipv_type": ipv_type,
                        "location": location,
                        "isp": isp,
                        "headers": headers,
                        "catchup": catchup,
                        "extra_info": extra_info
                    }
                    continue
                else:
                    continue

            url = normalized_url

            if url_origin not in retain_origin:
                if not skip_validation:
                    if not ipv_type:
                        if ipv_type_data and host in ipv_type_data:
                            ipv_type = ipv_type_data[host]
                        else:
                            ipv_type = ip_checker.get_ipv_type(url)
                            if ipv_type_data is not None:
                                ipv_type_data[host] = ipv_type

                    # 放开IP类型限制，所有链接都进测速
                    # if not check_ipv_type_match(ipv_type):
                    #     continue

                    # 放开地区、运营商限制，全部保留
                    # if location and location_list and not any(item in location for item in location_list):
                    #     continue
                    #
                    # if isp and isp_list and not any(item in isp for item in isp_list):
                    #     continue

            channel_list.append({
                "id": channel_id,
                "url": url,
                "host": host or get_url_host(url),
                "date": date,
                "delay": delay,
                "speed": speed,
                "resolution": resolution,
                "origin": url_origin,
                "ipv_type": ipv_type,
                "location": location,
                "isp": isp,
                "headers": headers,
                "catchup": catchup,
                "extra_info": extra_info
            })
            existing_map[url] = len(channel_list) - 1

        except Exception as e:
            print(t("msg.error_append_channel_data").format(info=e))
            continue


def append_old_data_to_info_data(info_data, cate, name, data, whitelist_maps=None, blacklist=None, ipv_type_data=None):
    def append_and_print(items, origin, label):
        if items:
            append_data_to_info_data(
                info_data, cate, name, items,
                origin=origin if origin else None,
                whitelist_maps=whitelist_maps,
                blacklist=blacklist,
                ipv_type_data=ipv_type_data
            )
        items_len = len(items)
        if items_len > 0:
            print(f"{label}: {items_len}", end=", ")

    whitelist_data = [item for item in data if item["origin"] == "whitelist"]
    append_and_print(whitelist_data, "whitelist", t("name.whitelist"))

    if open_local:
        local_data = [item for item in data if item["origin"] == "local"]
        append_and_print(local_data, "local", t("name.local"))

    if open_rtmp:
        hls_data = [item for item in data if item["origin"] == "hls"]
        append_and_print(hls_data, None, t("name.hls"))

    if open_history:
        history_data = [item for item in data if item["origin"] not in ["hls", "local", "whitelist"]]
        append_and_print(history_data, None, t("name.history"))


def print_channel_number(data: CategoryChannelData, cate: str, name: str):
    channel_list = data.get(cate, {}).get(name, [])
    print("IPv4:", len([c for c in channel_list if c["ipv_type"] == "ipv4"]), end=", ")
    print("IPv6:", len([c for c in channel_list if c["ipv_type"] == "ipv6"]), end=", ")
    print(f"{t('name.total')}:", len(channel_list))


def append_total_data(items, data, subscribe_result=None, whitelist_maps=None, blacklist=None):
    items = list(items)
    total_result = [("subscribe", subscribe_result)]
    unmatch_category = t("content.unmatch_channel")
    source_names = {
        format_channel_name(name)
        for cate, channel_obj in items
        if cate != unmatch_category
        for name in channel_obj.keys()
    }
    url_hosts_ipv_type = {}
    for obj in data.values():
        for value_list in obj.values():
            for value in value_list:
                if value_ipv_type := value.get("ipv_type"):
                    url_hosts_ipv_type[get_url_host(value["url"])] = value_ipv_type

    for cate, channel_obj in items:
        if cate == unmatch_category:
            for name, old_info_list in channel_obj.items():
                if old_info_list:
                    append_data_to_info_data(
                        data, cate, name, old_info_list,
                        whitelist_maps=whitelist_maps,
                        blacklist=blacklist,
                        ipv_type_data=url_hosts_ipv_type,
                        skip_validation=True
                    )
            continue

        for name, old_info_list in channel_obj.items():
            print(f"{name}:", end=" ")
            if old_info_list:
                append_old_data_to_info_data(data, cate, name, old_info_list, whitelist_maps, blacklist, url_hosts_ipv_type)
            for method, result in total_result:
                if config.open_method[method]:
                    name_results = get_channel_results_by_name(name, result)
                    append_data_to_info_data(
                        data, cate, name, name_results, origin=method,
                        whitelist_maps=whitelist_maps, blacklist=blacklist, ipv_type_data=url_hosts_ipv_type
                    )
                    print(f"{t(f'name.{method}')}: {len(name_results)}", end=", ")
            print_channel_number(data, cate, name)

    if config.open_unmatch_category and subscribe_result:
        unmatch_result = {
            name: info_list
            for name, info_list in subscribe_result.items()
            if name not in source_names
        }
        if unmatch_result:
            for name, info_list in unmatch_result.items():
                append_data_to_info_data(
                    data, unmatch_category, name, info_list,
                    origin="subscribe", whitelist_maps=whitelist_maps, blacklist=blacklist,
                    ipv_type_data=url_hosts_ipv_type, skip_validation=True
                )


def is_valid_speed_result(info) -> bool:
    try:
        delay = info.get("delay")
        if delay is None or delay == -1:
            return False

        res_str = info.get("resolution") or ""
        speed_val = info.get("speed", 0) or 0
        if not speed_val or math.isinf(speed_val):
            return False
        if open_filter_speed:
            if speed_val < resolution_speed_map.get(res_str, min_speed):
                return False

        if open_filter_resolution:
            try:
                res_value = get_resolution_value(res_str)
            except Exception:
                res_value = 0
            if res_value < min_resolution_value:
                return False

        return True
    except Exception:
        return False


async def test_speed(data, ipv6=False, callback=None, on_task_complete=None):
    ipv6_proxy_url = None if (not config.open_ipv6 or ipv6) else constants.ipv6_proxy
    open_headers = config.open_headers
    open_full_speed_test = config.open_full_speed_test
    get_resolution = config.open_filter_resolution and check_ffmpeg_installed_status()
    semaphore = asyncio.Semaphore(config.speed_test_limit)
    logger = get_logger(constants.speed_test_log_path, INFO, init=True)
    result_logger = get_logger(constants.result_log_path, INFO, init=True)

    async def limited_get_speed(channel_info):
        async with semaphore:
            headers = (open_headers and channel_info.get("headers")) or None
            return await get_speed(
                channel_info,
                headers=headers,
                ipv6_proxy=ipv6_proxy_url,
                filter_resolution=get_resolution,
                logger=logger,
            )

    total_tasks = sum(len(info_list) for channel_obj in data.values() for info_list in channel_obj.values())
    total_tasks_by_channel = defaultdict(int)
    for cate, channel_obj in data.items():
        for name, info_list in channel_obj.items():
            total_tasks_by_channel[(cate, name)] += len(info_list)
    completed = 0
    tasks = []
    channel_map = {}
    grouped_results = {}
    completed_by_channel = defaultdict(int)
    urls_limit = config.urls_limit
    valid_count_by_channel = defaultdict(int)

    def _cancel_remaining_channel_tasks(cate, name):
        for task, meta in list(channel_map.items()):
            if task.done():
                continue
            t_cate, t_name, _ = meta
            if t_cate == cate and t_name == name:
                try:
                    task.cancel()
                except Exception:
                    pass

    def _on_task_done(task):
        nonlocal completed
        try:
            result = task.result() if not task.cancelled() else {}
        except Exception:
            result = {}

        meta = channel_map.get(task)
        if not meta:
            return
        cate, name, info = meta
        if cate not in grouped_results:
            grouped_results[cate] = {}
        if name not in grouped_results[cate]:
            grouped_results[cate][name] = []
        merged = {**info, **result}
        grouped_results[cate][name].append(merged)

        if check_channel_need_frozen(merged):
            mark_url_bad(merged.get("url"))
        else:
            mark_url_good(merged.get("url"))

        is_valid = is_valid_speed_result(merged)
        if is_valid:
            valid_count_by_channel[(cate, name)] += 1
            if not open_full_speed_test and valid_count_by_channel[(cate, name)] >= urls_limit:
                _cancel_remaining_channel_tasks(cate, name)

            try:
                origin = merged.get('origin')
                origin_name = t(f"name.{origin}") if origin else origin
                result_logger.info(
                    f"ID: {merged.get('id')}, 频道: {name}, "
                    f"地址: {merged.get('url')}, 来源: {origin_name}, "
                    f"IP类型: {merged.get('ipv_type')}, 地区: {merged.get('location')}, 运营商: {merged.get('isp')}, "
                    f"延迟: {merged.get('delay') or -1}ms, 速度: {merged.get('speed') or 0:.2f}M/s, "
                    f"分辨率: {merged.get('resolution')}, FPS: {merged.get('fps') or t('name.unknown')}, "
                    f"视频: {merged.get('video_codec') or t('name.unknown')}, 音频: {merged.get('audio_codec') or t('name.unknown')}"
                )
            except Exception:
                pass

        completed += 1
        completed_by_channel[(cate, name)] += 1
        is_channel_last = completed_by_channel[(cate, name)] >= total_tasks_by_channel.get((cate, name), 0)
        is_last = completed >= total_tasks

        if on_task_complete:
            try:
                on_task_complete(cate, name, merged, is_channel_last, is_last, is_valid)
            except Exception:
                pass
        if callback:
            try:
                callback()
            except Exception:
                pass

    for cate, channel_obj in data.items():
        for name, info_list in channel_obj.items():
            for info in info_list:
                info['name'] = name
                task = asyncio.create_task(limited_get_speed(info))
                channel_map[task] = (cate, name, info)
                task.add_done_callback(_on_task_done)
                tasks.append(task)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    close_logger_handlers(logger)
    close_logger_handlers(result_logger)
    return grouped_results


def sort_channel_result(channel_data, result=None, filter_host=False, ipv6_support=True, cate=None, name=None):
    channel_result = defaultdict(lambda: defaultdict(list))
    categories = [cate] if cate else list(channel_data.keys())
    retain = retain_origin
    speed_lookup = get_speed_result
    sorter = get_sort_result
    unmatch_category = t("content.unmatch_channel")

    for c in categories:
        obj = channel_data.get(c, {}) or {}
        names = [name] if name else list(obj.keys())
        for n in names:
            values = obj.get(n) or []
            whitelist_result = []
            result_list = result.get(c, {}).get(n, []) if result else []

            if c == unmatch_category:
                seen_urls = set()
                for item in values:
                    url = item.get("url")
                    if url and url not in seen_urls:
                        channel_result[c][n].append(item)
                        seen_urls.add(url)
                continue

            if filter_host:
                merged_items = []
                for value in values:
                    origin = value.get("origin")
                    if origin in retain or (not ipv6_support and result and value.get("ipv_type") == "ipv6"):
                        whitelist_result.append(value)
                    else:
                        host = value.get("host")
                        merged = {**value, **(speed_lookup(host) or {})}
                        merged_items.append(merged)
                sorter_input = chain(result_list, merged_items) if merged_items else result_list
                total_result = whitelist_result + sorter(sorter_input, ipv6_support=ipv6_support)
            else:
                for value in values:
                    origin = value.get("origin")
                    if origin in retain or (not ipv6_support and result and value.get("ipv_type") == "ipv6"):
                        whitelist_result.append(value)
                total_result = whitelist_result + sorter(result_list, ipv6_support=ipv6_support)

            seen_urls = set()
            for item in total_result:
                url = item.get("url")
                if url and url not in seen_urls:
                    channel_result[c][n].append(item)
                    seen_urls.add(url)
    return channel_result


def generate_channel_statistic(logger, cate, name, values):
    total = len(values)
    valid_items = [v for v in values if is_valid_speed_result(v)]
    valid = len(valid_items)
    valid_rate = (valid / total * 100) if total > 0 else 0
    ipv4_count = len([v for v in values if v.get("ipv_type") == "ipv4"])
    ipv6_count = len([v for v in values if v.get("ipv_type") == "ipv6"])
    min_delay = min((v.get("delay") for v in values if (v.get("delay") or -1) != -1), default=-1)
    max_speed = max((v.get("speed") for v in values if (v.get("speed") or 0) > 0 and not math.isinf(v.get("speed"))), default=0)
    avg_speed = sum(v.get("speed", 0) for v in valid_items) / valid if valid else 0
    max_resolution = max((v.get("resolution") for v in values if v.get("resolution")), key=get_resolution_value, default="None")
    video_codecs = [v.get("video_codec") for v in values if v.get("video_codec")]
    audio_codecs = [v.get("audio_codec") for v in values if v.get("audio_codec")]
    fps_values = []
    for v in values:
        fps = v.get("fps")
        if fps and isinstance(fps, (int, float, str)):
            s = str(fps).replace(".", "")
            if s.isdigit():
                fps_values.append(float(fps))
    most_video = most_audio = None
    if video_codecs:
        most_video = Counter(video_codecs).most_common(1)[0][0]
    if audio_codecs:
        most_audio = Counter(audio_codecs).most_common(1)[0][0]
    avg_fps = sum(fps_values) / len(fps_values) if fps_values else None
    avg_fps_str = f"{avg_fps:.2f}" if avg_fps is not None else t("name.unknown")
    video_str = most_video or t("name.unknown")
    audio_str = most_audio or t("name.unknown")

    if config.open_full_speed_test:
        content = (f"分类: {cate}, 频道: {name}, 总数: {total}, 有效: {valid}, 有效率: {valid_rate:.2f}%, "
                   f"IPv4: {ipv4_count}, IPv6: {ipv6_count}, 最低延迟: {min_delay}ms, 最高速度: {max_speed:.2f}M/s, "
                   f"平均速度: {avg_speed:.2f}M/s, 最大分辨率: {max_resolution}, 平均FPS: {avg_fps_str}, "
                   f"视频编码: {video_str}, 音频编码: {audio_str}")
    else:
        content = (f"分类: {cate}, 频道: {name}, 有效: {valid}, IPv4: {ipv4_count}, IPv6: {ipv6_count}, "
                   f"最低延迟: {min_delay}ms, 最高速度: {max_speed:.2f}M/s, 平均速度: {avg_speed:.2f}M/s, "
                   f"最大分辨率: {max_resolution}, 平均FPS: {avg_fps_str}, 视频编码: {video_str}, 音频编码: {audio_str}")
    logger.info(content)
    print(f"📊 {content}")


def process_write_content(
        path: str, data: CategoryChannelData, hls_url=None, open_empty_category=False,
        ipv_type_prefer=None, origin_type_prefer=None, first_channel_name=None, enable_log=False, is_last=False
):
    content = ""
    no_result_name = []
    first_cate = True
    result_data = defaultdict(list)
    custom_print.disable = not enable_log
    rtmp_type = ["hls"] if hls_url else []
    open_url_info = config.open_url_info
    unmatch_category = t("content.unmatch_channel")

    for cate, channel_obj in data.items():
        content += f"{'\n\n' if not first_cate else ''}{cate},#genre#"
        first_cate = False
        for name in channel_obj:
            info_list = data.get(cate, {}).get(name, [])
            channel_urls = _get_total_urls_cached(
                info_list, ipv_type_prefer, origin_type_prefer, rtmp_type,
                apply_limit=(cate != unmatch_category)
            )
            result_data[name].extend(channel_urls)
            if not channel_urls:
                if open_empty_category:
                    no_result_name.append(name)
                continue
            for item in channel_urls:
                item_url = item["url"]
                if open_url_info and item["extra_info"]:
                    item_url = add_url_info(item_url, item["extra_info"])
                total_item_url = f"{hls_url}/{item['id']}.m3u8" if hls_url else item_url
                content += f"\n{name},{total_item_url}"

    if open_empty_category and no_result_name and is_last:
        custom_print(f"\n{t('msg.no_result_channel')}")
        content += f"\n\n{t('content.no_result_channel')},#genre#"
        for i, name in enumerate(no_result_name):
            end = ", " if i < len(no_result_name)-1 else ""
            custom_print(name, end=end)
            content += f"\n{name},url"

    if config.open_update_time:
        update_item = {"id": "id", "url": "url"}
        for channel_obj in data.values():
            for info_list in channel_obj.values():
                urls = _get_total_urls_cached(info_list, ipv_type_prefer, origin_type_prefer, rtmp_type, True)
                if urls:
                    update_item = urls[0]
                    break
            if update_item["id"] != "id":
                break
        now = get_datetime_now()
        update_url = update_item["url"]
        if open_url_info and update_item["extra_info"]:
            update_url = add_url_info(update_url, update_item["extra_info"])
        final_url = f"{hls_url}/{update_item['id']}.m3u8" if hls_url else update_url
        title = t("content.update_time") if is_last else t("content.update_running")
        if config.update_time_position == "top":
            content = f"{title},#genre#\n{now},{final_url}\n\n{content}"
        else:
            content += f"\n\n{title},#genre#\n{now},{final_url}"

    if hls_url:
        db_dir = os.path.dirname(constants.rtmp_data_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        try:
            ensure_result_data_schema(constants.rtmp_data_path)
            conn = get_db_connection(constants.rtmp_data_path)
            with conn.cursor() as cur:
                cur.execute('''CREATE TABLE IF NOT EXISTS result_data
                             (id TEXT PRIMARY KEY, url TEXT, headers TEXT,
                              video_codec TEXT, audio_codec TEXT, resolution TEXT, fps REAL)''')
                for lst in result_data.values():
                    for item in lst:
                        cur.execute('''INSERT OR REPLACE INTO result_data
                                     (id, url, headers, video_codec, audio_codec, resolution, fps)
                                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                    (str(item.get("id")), item.get("url"), json.dumps(item.get("headers")),
                                     item.get("video_codec"), item.get("audio_codec"),
                                     item.get("resolution"), item.get("fps")))
            conn.commit()
            return_db_connection(constants.rtmp_data_path, conn)
        except Exception:
            pass

    try:
        target_dir = os.path.dirname(path) or "."
        os.makedirs(target_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, dir=target_dir,
                                         prefix=os.path.basename(path)+".tmp.") as f:
            f.write(content)
            tmp = f.name
        os.replace(tmp, path)
        os.chmod(path, 0o644)
    except Exception:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    try:
        convert_to_m3u(path, first_channel_name, data=result_data)
    except Exception:
        pass


def write_channel_to_file(data, ipv6=False, first_channel_name=None, skip_print=False, is_last=False):
    if not skip_print:
        print(t("msg.writing_result"), flush=True)

    open_empty_category = config.open_empty_category
    ipv_type_prefer = list(config.ipv_type_prefer)
    if any(p == "auto" for p in ipv_type_prefer):
        ipv_type_prefer = ["ipv6", "ipv4"] if ipv6 else ["ipv4", "ipv6"]
    origin_type_prefer = config.origin_type_prefer
    hls_url = f"{get_public_url()}/hls" if config.open_rtmp else None

    file_list = [
        {"path": config.final_file, "enable_log": True},
        {"path": constants.ipv4_result_path, "ipv_type_prefer": ["ipv4"]},
        {"path": constants.ipv6_result_path, "ipv_type_prefer": ["ipv6"]},
    ]
    if config.open_rtmp and not os.getenv("GITHUB_ACTIONS"):
        file_list.extend([
            {"path": constants.hls_result_path, "hls_url": hls_url},
            {"path": constants.hls_ipv4_result_path, "hls_url": hls_url, "ipv_type_prefer": ["ipv4"]},
            {"path": constants.hls_ipv6_result_path, "hls_url": hls_url, "ipv_type_prefer": ["ipv6"]},
        ])

    for fcfg in file_list:
        d = os.path.dirname(fcfg["path"])
        if d:
            os.makedirs(d, exist_ok=True)
        process_write_content(
            path=fcfg["path"],
            data=data,
            hls_url=fcfg.get("hls_url"),
            open_empty_category=open_empty_category,
            ipv_type_prefer=fcfg.get("ipv_type_prefer", ipv_type_prefer),
            origin_type_prefer=origin_type_prefer,
            first_channel_name=first_channel_name,
            enable_log=fcfg.get("enable_log", False),
            is_last=is_last
        )

    if not skip_print:
        print(t("msg.write_success"), flush=True)
