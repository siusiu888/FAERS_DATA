from src.crawler import fetch_pages
from src.utils import load_failed_pages, save_failed_page, csv_save, summary

if __name__ == "__main__":
    target_page = 20
    url = "https://books.toscrape.com"

    failed_urls = load_failed_pages()

    if failed_urls:
        print("检测到失败页面，开始断点续传...")
        all_rows, failed_page = fetch_pages(
            target_page, resume_urls=failed_urls
        )
    else:
        all_rows, failed_page = fetch_pages(target_page)

    csv_save(all_rows)
    summary(target_page, all_rows, failed_page)
    save_failed_page(failed_page)
