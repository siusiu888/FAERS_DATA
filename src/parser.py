
from bs4 import BeautifulSoup

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
    return rows#每一页的书籍信息
