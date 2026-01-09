import os
import csv

def save_failed_page(failed_page,filename= "failed_page.txt"):
    if not failed_page:
        return
    with open (filename,"w",encoding="utf-8") as f:
        for url in failed_page:
            f.write(url+"\n")

def load_failed_pages(filename="failed_page.txt"):
    if not os.path.exists(filename):
        # 如果文件不存在，直接返回一个空列表
        return[]

    # 2. 如果文件存在，则打开并读取它
    with open(filename,"r",encoding="utf-8")as f:
        return[line.strip() for line in f if line.strip()]
    #for line in f:
    #检查这一行是否为空行,line.strip() 会移除换行符，如果移除后变成了空字符串，说明它原本就是个空行
    #     if line.strip():  # 如果不是空行,处理这一行（去除首尾空白），并添加到列表中
        #     clean_line = line.strip()
        #     result_list.append(clean_line)
    # return result_list
    
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

def summary(target_page,all_rows,failed_page):
     print("\n===== 运行总结 =====")
     print("页数：",target_page)
     print("记录总数：",len(all_rows))
     print("失败总数：",len(failed_page))
     if failed_page:
        print("\n===== 失败页面 =====")
        failed_page.sort()
        for url in failed_page:
            if "page-" in url:
                page_no = url.split("page-")[1].split(".")[0]
                print(f"-- 第 {page_no} 页获取失败 ({url})")
            else:
                print(f"-- 首页获取失败 ({url})")
     print("输出文件：BOOK_DATA.csv")
     print("====================\n")