import requests
import time
from bs4 import BeautifulSoup
from src.parser import fetch_perpages_books

# ----------得到请求头----------

def fetch_url(url):
    session = requests.Session()#先建浏览器
    max_retries=3
    
    for attempt in range(1,max_retries+1):
        try:
            headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
}
            r = session.get(url,timeout=10)
            r.raise_for_status()
            time.sleep(0.5)
            soup = BeautifulSoup(r.text,"lxml")#解析结果
            return soup
        except requests.RequestException as e :
            print (f"请求失败：{e}")

            if attempt < max_retries:
                time.sleep(attempt)
            else:
                print(f"{url}请求失败")
                
                return None


#------------调度层------------
def fetch_pages(target_page,resume_urls=None):
    all_rows = []      # 存储书籍信息
    failed_page = []   # 存储失败的URL
    seen_urls = set()  # 去重集合
    # 确定要抓取的任务清单
    if resume_urls:
        #判断这个列表是不是空的，空的就是False,有内容的就是Ture
        print(f"正在恢复抓取，共有 {len(resume_urls)} 个页面待重试...")
        page_urls = resume_urls
    else:
        page_urls =[]
        for page in range(1,target_page+1):
            if page == 1:
                    page_urls.append(url)
            else:
                page_urls.append(
                    f"https://books.toscrape.com/catalogue/page-{page}.html"
                )

    for page_url in page_urls:
        page_text = fetch_url(page_url)
            # 在“调用方”，判断返回值是否为 None    
        if page_text is None:
            failed_page.append(page_url)
            continue
        
        page_books =fetch_perpages_books(page_text)
            #进行去重
        for book in page_books:
            product_url = book["product_url"]
            if product_url in seen_urls:
                continue
            else:
                seen_urls.add(product_url)
                all_rows.append(book)
        
        
    return all_rows ,failed_page

