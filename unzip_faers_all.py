import zipfile
from pathlib import Path

# ======== 路径配置 ========

FAILED_LOG = Path(r"C:\Users\venture\first\FAERS_DATA\unzip_failed_files.txt")
RETRY_FAILED_LOG = Path(r"C:\Users\venture\first\FAERS_DATA\unzip_failed_files_retry_failed.txt")

UNZIP_ROOT = Path(r"C:\Users\venture\first\FAERS_DATA\UNZIP_DATA")

# ======== 主逻辑 ========

def retry_failed_unzip():
    if not FAILED_LOG.exists():
        print("未找到失败记录文件，无法重试")
        return

    retry_failed = []

    with open(FAILED_LOG, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # 只取 ZIP 路径行
    zip_paths = [line.strip() for line in lines if line.strip().lower().endswith(".zip")]

    if not zip_paths:
        print("失败记录中未找到 ZIP 路径")
        return

    for zip_str in zip_paths:
        zip_path = Path(zip_str)

        if not zip_path.exists():
            print(f"[不存在] {zip_path}")
            retry_failed.append((zip_path, "ZIP 文件不存在"))
            continue

        try:
            # 从路径中解析 year / quarter
            # ...\RAW_ZIP\2011\Q3\xxx.zip
            year = zip_path.parents[2].name
            quarter = zip_path.parents[1].name

            target_dir = UNZIP_ROOT / year / quarter
            target_dir.mkdir(parents=True, exist_ok=True)

            print(f"[重试解压] {zip_path} -> {target_dir}")

            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(target_dir)

        except Exception as e:
            print(f"[仍失败] {zip_path}")
            retry_failed.append((zip_path, repr(e)))

    # 写入仍失败的文件
    if retry_failed:
        with open(RETRY_FAILED_LOG, "w", encoding="utf-8") as f:
            f.write("FAERS 解压重试失败记录\n")
            f.write("=" * 60 + "\n")
            for zp, err in retry_failed:
                f.write(f"{zp}\n")
                f.write(f"ERROR: {err}\n")
                f.write("-" * 60 + "\n")

        print(f"\n部分文件仍失败，已记录到：{RETRY_FAILED_LOG}")
    else:
        print("\n所有失败文件已成功解压")

# ======== 程序入口 ========

if __name__ == "__main__":
    retry_failed_unzip()
