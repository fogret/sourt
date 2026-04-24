import asyncio
import re
import subprocess
from time import time

from utils.i18n import t

min_measure_time = 1.0
stability_window = 4
stability_threshold = 0.12


def check_ffmpeg_installed_status():
    """
    Check ffmpeg is installed
    """
    status = False
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        status = result.returncode == 0
    except FileNotFoundError:
        status = False
    except Exception as e:
        print(e)
    finally:
        print(t("msg.ffmpeg_installed") if status else t("msg.ffmpeg_not_installed"))
        return status


async def ffmpeg_url(url, headers=None, timeout=10):
    """
    Async wrapper that runs ffmpeg similar to old implementation and returns stderr output as text.
    """
    headers_str = "".join(f"{k}: {v}\r\n" for k, v in (headers or {}).items())

    args = ["ffmpeg", "-t", str(timeout)]
    if headers_str:
        args += ["-headers", headers_str]
    args += ["-http_persistent", "0", "-stats", "-i", url, "-f", "null", "-"]

    proc = None
    stderr_parts: list[bytes] = []
    speed_samples: list[float] = []
    bitrate_re = re.compile(r"bitrate=\s*([0-9\.]+)\s*k?bits/s", re.IGNORECASE)
    start = time()

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        while True:
            try:
                line = await asyncio.wait_for(proc.stderr.readline(), timeout=0.5)
            except asyncio.TimeoutError:
                line = b""

            elapsed = time() - start
            if not line:
                # 进程已退出 或 超时终止
                if proc.returncode is not None:
                    break
                if elapsed >= timeout:
                    break
                await asyncio.sleep(0)
                continue

            stderr_parts.append(line)
            text = line.decode("ignore")

            # 解析码率
            m = bitrate_re.search(text)
            if m:
                try:
                    kbps = float(m.group(1))
                    mbps = kbps / 8.0 / 1024.0
                    speed_samples.append(mbps)
                except Exception:
                    pass

            # 稳定性判定 提前终止
            if elapsed >= min_measure_time and len(speed_samples) >= stability_window:
                window = speed_samples[-stability_window:]
                mean = sum(window) / len(window)
                if mean > 0 and (max(window) - min(window)) / mean < stability_threshold:
                    break

    except Exception:
        pass
    finally:
        # 统一安全销毁进程，杜绝僵尸进程
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
            await proc.wait()

        # 读取剩余未消费的stderr
        if proc:
            remain_err = await proc.stderr.read()
            stderr_parts.append(remain_err)

    stderr_bytes = b"".join(stderr_parts)
    return stderr_bytes.decode("ignore")
