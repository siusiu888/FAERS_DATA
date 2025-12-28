
from traceback import print_exception
from turtle import title
import requests
from bs4 import BeautifulSoup
from urllib3 import response
import csv


# ----------得到请求头----------

def fetch_url(url):
    session = requests.Session()#先建浏览器
    r = session.get(url)
    print("状态码：",r.status_code)
    soup = BeautifulSoup(r.text,"lxml")#解析结果
    return soup

 # ---------查找一本书获得目标标签------------

def fetch_perpages_books (soup):
    rows=[]
    book_nodes = soup.find_all("article",class_ = "product_pod")#返回的不是书而是节点
    page_no = soup.find("li",class_="current").text.split()[1]
    for book in book_nodes:
        title = book.find("a",title=True)["title"]
        price = book.find("p",class_="price_color").text
        availability = book.find("p",class_="instock availability").text.strip()#因为有换行和空格
        rating = book.find("p",class_="star-rating")["class"][1]
        product_url = "url"+book.find("a")['href']
        rows.append({
            "title": title,
            "price": price,
            "availability": availability,
            "rating": rating,
            "product_url": product_url,
            "page_no": page_no,}

        )
    return rows


#------------调度层------------
def fetch_pages(target_page):
    all_rows=[]
    for page in range(1,target_page+1):
        if page == 1:
            page_url = url
        else:
            page_url = f"https://books.toscrape.com/catalogue/page-{page}.html"
        page_text = fetch_url(page_url)
        page_books =fetch_perpages_books(page_text)
        all_rows.extend(page_books)

    return all_rows 

def csv_save(all_rows):

    if not all_rows:
        print("没有数据")
        return
    else:
        with open ('BOOK_DATA.csv',mode='w',encoding='utf-8',newline='') as f:
            fieldnames = all_rows[0].keys()
            writer = csv.DictWriter(f,fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    print("数据已保存")

def summary(target_page,all_rows):
     print("\n===== 运行总结 =====")
     print("页数：",target_page)
     print("记录总数：",len(all_rows))
     print("输出文件：BOOK_DATA.csv")
     print("====================\n")


if __name__ == "__main__":
    target_page = 20
    url = "https://books.toscrape.com"
    all_rows = fetch_pages(target_page)
    csv_save(all_rows)
    summary(target_page, all_rows)
