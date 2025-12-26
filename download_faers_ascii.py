import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ======================
# 配置区
# ======================
SAVE_ROOT = Path(r"C:\Users\venture\first\FAERS_DATA\RAW_ZIP")
START_YEAR = 2004
END_YEAR = 2025

MAIN_PAGE = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
OLDER_PAGE = "https://www.fda.gov/drugs/fdas-adverse-event-reporting-system-faers/adverse-event-reporting-system-aers-older-quarterly-data-files"

TIMEOUT = (10, 60)

# ======================
# 会话（禁用系统代理）
# ======================
def make_session():
    s = requests.Session()
    s.trust_env = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0"
    })
    return s


# ======================
# 抓 ASCII ZIP 链接
# ======================
def collect_ascii_links(session, page_url):
    html = session.get(page_url, timeout=TIMEOUT).text
    soup = BeautifulSoup(html, "lxml")

    links = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if not href.lower().endswith(".zip"):
            continue

        full_url = urljoin(page_url, href)
        name = Path(urlparse(full_url).path).name.lower()

        if "ascii" in name:
            links.append(full_url)

    return links


# ======================
# 解析年份 / 季度
# ======================
def parse_year_quarter(filename):
    m = re.search(r"_(\d{4})q([1-4])\.zip$", filename.lower())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


# ======================
# 下载
# ======================
def download(session, url, save_path, max_retries=5):
    save_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            headers = {}
            downloaded = 0

            if save_path.exists():
                downloaded = save_path.stat().st_size
                headers["Range"] = f"bytes={downloaded}-"

            with session.get(url, stream=True, headers=headers, timeout=(10, 120)) as r:
                if r.status_code not in (200, 206):
                    raise RuntimeError(f"HTTP {r.status_code}")

                mode = "ab" if downloaded > 0 else "wb"
                total = r.headers.get("Content-Length")
                total = int(total) + downloaded if total else None

                with open(save_path, mode) as f, tqdm(
                    initial=downloaded,
                    total=total,
                    unit="B",
                    unit_scale=True,
                    desc=save_path.name,
                ) as bar:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            return True

        except Exception as e:
            print(f"[RETRY {attempt}/{max_retries}] {save_path.name} -> {e}")
            if attempt == max_retries:
                return False



# ======================
# 主流程
# ======================
def main():
    session = make_session()

    print("抓取主页面（含最新季度 + 2012 Q4）")
    links_main = collect_ascii_links(session, MAIN_PAGE)

    print("抓取 Older Files（含 2012 Q1–Q3）")
    links_old = collect_ascii_links(session, OLDER_PAGE)

    all_links = links_main + links_old

    tasks = []
    for url in all_links:
        fname = Path(urlparse(url).path).name
        yq = parse_year_quarter(fname)
        if not yq:
            continue

        year, quarter = yq
        if START_YEAR <= year <= END_YEAR:
            tasks.append((year, quarter, fname, url))

    tasks.sort(key=lambda x: (x[0], x[1]))

    print(f"共找到 {len(tasks)} 个 ASCII ZIP（{START_YEAR}–{END_YEAR}）")

    for year, quarter, fname, url in tasks:
        save_path = SAVE_ROOT / str(year) / f"Q{quarter}" / fname

        if save_path.exists():
            print(f"[SKIP] {year} Q{quarter}")
            continue

        print(f"[DOWN] {year} Q{quarter}")
        ok = download(session, url, save_path)
        if ok:
            print(f"[OK]   {save_path}")
        else:
            print(f"[MISS] {fname}")

    print("=== 完成 ===")


if __name__ == "__main__":
    main()
