# 实现一个爬虫流程：
# 抓取 page-1 到 page-10
# 抓取失败的页面写入 failed_pages.txt
# 第二次运行时：
# 只抓 failed_pages.txt 里的 URL
# 成功的从失败列表中移除
# 仍失败的继续保留





from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


#------全局配置-------

BASE_url="https://books.toscrape.com/catalogue/page-{}.html"

FAILED_FILE = "failed_files.txt" 
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
}

def create_session_with_retries(max_retries):
    session = requests.Session()
    
    retry = Retry(
        total = max_retries,
        backoff_factor = 1,
        status_forcelist =[429,500,502,503,504],
        allowed_methods = {"HEAD","GET","OPTIONS"}
        )
    adapter = HTTPAdapter(
        max_retries = retry,
        pool_connections = 10,
        pool_maxsize = 10,
        pool_block = False
    )
    #挂载适配器到session
    session.mount("http://",adapter)
    session.mount("https://",adapter)
    return session
    

def safe_get_page(session,page_url):
    try:
        s =session.get(page_url,headers = HEADERS,timeout = 10)
        s.raise_for_status()
        soup = BeautifulSoup(s.text,"lxml")
        return soup
    except requests.exceptions.RequestException as e:
        print(f"抓取失败: {page_url}, 原因: {e}")
        return None

# 我们让它接收一个URL列表，而不是目标页数        
def fetch_pages_soup(session,urls_list):
    successful_pages =[]
    failed_urls = []
    for page_url in urls_list:
        soup = safe_get_page(session, page_url)

        if soup is None:
            failed_urls.append(page_url)
        else:
            successful_pages.append(soup)

    return successful_pages,failed_urls


def write_failed_urls(failed_urls):
    unique_urls = list(set(failed_urls))
    #通过 “覆盖写入”，实现 “成功的 URL 被移除，失败的 URL 被保留”。
    with open("failed_files.txt","w",encoding="utf-8") as f:
        for file in unique_urls:
            f.write(file+'\n') # 写入一行，需手动加换行符 \n


#从保存的文件中，读取上次爬取进度
def read_failed_urls():
    urls_set = set()
    #程序在启动时会检查 failed_files.txt 是否存在。
    # 如果存在，它就会进入 “重试模式”，只抓取从文件里读到的 URL。
    if not os.path.exists("FAILED_FILE"):
        return[]
    else:
        with open("failed_files.txt","r",encoding = "utf-8") as f:
        #去除空行空字符串
            for line in f:#在文本模式下，文件对象的迭代是按行进行的。
                cleaned_line = line.strip()
                if cleaned_line :
                    urls_set.add(cleaned_line)

    return list(urls_set)

# ==============================================================================
# 2. 调度函数 (核心逻辑)
# ==============================================================================
def run_crawler(session,target_page):
    print("\n--- Crawler Scheduler Started ---")  # 【★新增】启动提示
    if os.path.exists("failed_files.txt"):
        print("--- Mode: Retry Failed URLs ---")   # 【★新增】模式提示
        urls_to_fetch = read_failed_urls()
        if not urls_to_fetch:
            print("--- No failed URLs to retry. Exiting scheduler. ---")
            return 
    else:
        print("--- Mode: Initial Run ---")
        urls_to_fetch = []
        for i in range(1,target_page+1):
            URLs = BASE_url.format(i)
            urls_to_fetch.append(URLs)
  # 执行阶段：执行抓取任务
    successful_pages, newly_failed_urls = fetch_pages_soup(session, urls_to_fetch)
    write_failed_urls(newly_failed_urls)
    print(f"本次成功: {len(successful_pages)} 页")
    print(f"本次失败: {len(newly_failed_urls)} 页")


if __name__ == "__main__":  #只放“程序启动时必须发生的事情”。
    session = create_session_with_retries(max_retries=3)
    # 程序一启动，就必须有一个 session
    # session 是运行环境的一部分
    # 不创建，程序无法工作
    run_crawler(session, target_page=10)
    # 它是整个程序的“入口任务”，不调用它，程序什么都不做


























def main():
    """程序的主入口，负责初始化和资源清理。"""
    print("--- Program Started ---")
    session = None
    try:
        # 初始化
        session = create_session_with_retries()
        
        # 启动核心调度逻辑
        run_crawler(session)

    except KeyboardInterrupt:
        print("\n--- Program interrupted by user. Exiting. ---")
    except Exception as e:
        print(f"\n--- Fatal Error: An unexpected error occurred: {e} ---")
    finally:
        # 清理
        if session:
            session.close()
            print("--- Session closed. ---")
    print("--- Program Ended ---")

# 程序启动点
if __name__ == "__main__":
    main()