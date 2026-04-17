import requests
import re
import sys
import time
import os
from datetime import datetime, timedelta

OWNER = "Guovin"
REPO = "iptv-api"

TOKEN = os.getenv("GH_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}

def beijing_time():
    """返回北京时间（UTC+8）"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 8*3600))

def log(msg):
    ts = beijing_time()
    sys.stdout.write(f"[{ts}] {msg}\n")
    sys.stdout.flush()

# -----------------------------
# 获取 forks
# -----------------------------
def get_forks(page):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/forks?page={page}&per_page=100&sort=newest"
    r = requests.get(url, headers=HEADERS, timeout=10)
    if r.status_code != 200:
        log(f"❌ 获取 forks 失败：HTTP {r.status_code}")
        return []
    return r.json()

# -----------------------------
# 获取最近 7 天 commit 数
# -----------------------------
def get_recent_commits(user, repo, branch):
    url = f"https://api.github.com/repos/{user}/{repo}/commits?sha={branch}&per_page=20"
    r = requests.get(url, headers=HEADERS, timeout=10)

    if r.status_code != 200:
        log(f"  ⚠ GitHub API 错误：HTTP {r.status_code}")
        return 0

    data = r.json()

    if isinstance(data, dict):
        log(f"  ⚠ GitHub 返回错误：{data.get('message')}")
        return 0

    cutoff = datetime.utcnow() - timedelta(days=7)
    count = 0

    for c in data:
        try:
            date = c["commit"]["committer"]["date"]
            dt = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            if dt >= cutoff:
                count += 1
        except:
            continue

    return count

# -----------------------------
# 获取 subscribe.txt
# -----------------------------
def fetch_subscribe(user, repo, branch):
    raw_url = f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/config/subscribe.txt"
    try:
        r = requests.get(raw_url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.text
    except:
        pass
    return ""

# -----------------------------
# 提取 URL
# -----------------------------
def extract_urls(text):
    return re.findall(r'https?://[^\s]+', text)

# -----------------------------
# 测试 URL 是否可访问
# -----------------------------
def test_url(url):
    try:
        r = requests.get(url, timeout=5)
        if r.status_code < 400:
            return True
    except:
        pass
    return False

# -----------------------------
# 主流程
# -----------------------------
def main():
    log("===== 开始筛选活跃 forks（7 天内 ≥ 7 次更新） =====")
    log(f"北京时间：{beijing_time()}")

    all_urls = []
    active_projects = []
    page = 1

    # ⭐ 不再创建 output 目录（你已有）

    while True:
        forks = get_forks(page)
        if not forks:
            break

        for f in forks:
            full = f["full_name"]
            user, repo = full.split("/")
            branch = f["default_branch"]

            log(f"\n检查 fork：{full}（分支：{branch}）")

            commit_count = get_recent_commits(user, repo, branch)
            log(f"  → 最近 7 天 commit 数：{commit_count}")

            if commit_count < 7:
                log("  ❌ 不满足条件，跳过")
                continue

            log("  ✔ 满足条件，读取 subscribe.txt")

            # ⭐ 记录项目地址
            active_projects.append(f"https://github.com/{user}/{repo}")

            content = fetch_subscribe(user, repo, branch)
            if not content:
                log("    ❌ 未找到 subscribe.txt")
                continue

            urls = extract_urls(content)
            if not urls:
                log("    ⚠ subscribe.txt 中无 URL")
                continue

            log(f"    ✔ 提取到 {len(urls)} 个地址，开始测试可用性...")

            for u in urls:
                if test_url(u):
                    log(f"      ✔ 可访问：{u}")
                    all_urls.append(u)
                else:
                    log(f"      ❌ 不可访问：{u}")

        page += 1

    log("\n正在去重...")
    all_urls = list(dict.fromkeys(all_urls))
    active_projects = list(dict.fromkeys(active_projects))
    log(f"✔ 去重后剩余 {len(all_urls)} 个可用地址")
    log(f"✔ 活跃项目：{len(active_projects)} 个")

    # ⭐ 写项目地址文件
    with open("output/projects.txt", "w", encoding="utf-8") as f:
        f.write(f"# 更新时间（北京时间）：{beijing_time()}\n")
        f.write("\n".join(active_projects))

    # ⭐ 写 URL 文件
    with open("output/urls.txt", "w", encoding="utf-8") as f:
        f.write(f"# 更新时间（北京时间）：{beijing_time()}\n")
        f.write("\n".join(all_urls))

    log("✔ 已保存 output/projects.txt（项目地址）")
    log("✔ 已保存 output/urls.txt（可用 URL）")
    log("===== 扫描完成 =====")

if __name__ == "__main__":
    main()
