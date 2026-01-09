import os
import sys
import gc
import csv
import json
import time
import glob
import logging
import traceback
from datetime import datetime
from multiprocessing import get_context, current_process

import pandas as pd


# =========================================================
# 0) 用户可配置项（你主要改这里）
# =========================================================

BASE_DIR = r"C:\Users\venture\first\FAERS_DATA"

# 输入根目录：UNZIP_DATA\{year}\{Q1..Q4}\ascii\*.txt
INPUT_ROOT = os.path.join(BASE_DIR, "UNZIP_DATA")

# 输出根目录：CSV_DATA\{year}\{Q1..Q4}\*.csv
OUTPUT_ROOT = os.path.join(BASE_DIR, "CSV_DATA")

# 日志根目录：LOGS\faers_decode\run_时间戳\
LOG_ROOT = os.path.join(BASE_DIR, "LOGS", "faers_decode")

# 是否跳过已存在的输出（建议 True，方便断点续跑）
SKIP_EXISTING = True

# 并行进程数：None 自动；或手动填 2/4/8
PROCESS_NUM = None

# 重试（包含首次）：3 = 最多尝试 3 次
MAX_RETRIES = 3
BASE_BACKOFF_SEC = 2

# 大文件分块阈值与每块行数
CHUNK_THRESHOLD_MB = 300
CHUNK_ROWS = 300_000

# FAERS ASCII 常见设置
INPUT_ENCODING = "latin1"
DELIM = "$"

# 你要处理的表（按前缀过滤）
TABLE_PREFIXES = {"DEMO", "DRUG", "INDI", "OUTC", "REAC", "RPSR", "STAT", "THER"}

# 清理策略
DROP_ALL_EMPTY_COLS = True
STRIP_WHITESPACE = True
FILL_NA_WITH_EMPTY = True


# =========================================================
# 1) 运行目录（每次运行单独目录，避免日志/报告混乱）
# =========================================================

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = os.path.join(LOG_ROOT, f"run_{RUN_TS}")
os.makedirs(RUN_DIR, exist_ok=True)


# =========================================================
# 2) 主进程 logger
# =========================================================

def build_main_logger() -> logging.Logger:
    logger = logging.getLogger("MAIN")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [MAIN] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(os.path.join(RUN_DIR, f"main_{RUN_TS}.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# =========================================================
# 3) 前置校验
# =========================================================

def basic_file_validate(path: str) -> (bool, str):
    if not os.path.exists(path):
        return False, "NOT_FOUND"
    try:
        size = os.path.getsize(path)
    except OSError:
        return False, "CANNOT_STAT"
    if size == 0:
        return False, "EMPTY_FILE"

    # 轻量检查：前 4KB 是否出现 '$'（只 warning，不作为硬失败）
    try:
        with open(path, "rb") as f:
            head = f.read(4096)
        if b"$" not in head:
            return True, "NO_DELIM_IN_HEAD_WARN"
    except Exception:
        return False, "CANNOT_READ_HEAD"

    return True, "OK"


# =========================================================
# 4) pandas read_csv 兼容封装
# =========================================================

def read_faers_full(path: str) -> pd.DataFrame:
    common_kwargs = dict(
        sep=DELIM,
        encoding=INPUT_ENCODING,
        dtype=str,
        engine="python",
        quoting=csv.QUOTE_NONE,
        keep_default_na=False,
        na_values=[],
    )
    try:
        return pd.read_csv(path, encoding_errors="replace", on_bad_lines="warn", **common_kwargs)
    except TypeError:
        return pd.read_csv(path, **common_kwargs)


def read_faers_chunks(path: str):
    common_kwargs = dict(
        sep=DELIM,
        encoding=INPUT_ENCODING,
        dtype=str,
        engine="python",
        quoting=csv.QUOTE_NONE,
        keep_default_na=False,
        na_values=[],
        chunksize=CHUNK_ROWS,
    )
    try:
        return pd.read_csv(path, encoding_errors="replace", on_bad_lines="warn", **common_kwargs)
    except TypeError:
        return pd.read_csv(path, **common_kwargs)


# =========================================================
# 5) 清理
# =========================================================

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    if DROP_ALL_EMPTY_COLS:
        df = df.dropna(axis=1, how="all")
    if FILL_NA_WITH_EMPTY:
        df = df.fillna("")
    if STRIP_WHITESPACE:
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df


# =========================================================
# 6) 安全输出（tmp -> replace）
# =========================================================

def atomic_write_csv(df: pd.DataFrame, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp_path = out_path + ".tmp"
    df.to_csv(tmp_path, index=False, encoding="utf-8")
    os.replace(tmp_path, out_path)


def atomic_write_csv_chunks(chunks, out_path: str, logger: logging.Logger) -> (int, int):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp_path = out_path + ".tmp"

    total_rows = 0
    cols = None
    first = True

    for chunk in chunks:
        chunk = clean_df(chunk)

        if cols is None:
            cols = chunk.shape[1]
        elif chunk.shape[1] != cols:
            logger.warning(f"Chunk column mismatch: expected={cols}, got={chunk.shape[1]} -> align by reindex")
            chunk = chunk.reindex(columns=list(range(cols)), fill_value="")

        chunk.to_csv(
            tmp_path,
            mode="w" if first else "a",
            header=first,
            index=False,
            encoding="utf-8"
        )
        first = False
        total_rows += len(chunk)

    if first:
        pd.DataFrame().to_csv(tmp_path, index=False, encoding="utf-8")
        cols = 0

    os.replace(tmp_path, out_path)
    return total_rows, (cols or 0)


# =========================================================
# 7) 任务发现：扫描所有年份/季度/ascii/*.txt
# =========================================================

def discover_tasks(main_logger: logging.Logger):
    tasks = []

    if not os.path.isdir(INPUT_ROOT):
        raise FileNotFoundError(f"INPUT_ROOT not found: {INPUT_ROOT}")

    # 年份目录：只取 4 位数字的文件夹
    years = sorted(
        d for d in os.listdir(INPUT_ROOT)
        if os.path.isdir(os.path.join(INPUT_ROOT, d)) and d.isdigit() and len(d) == 4
    )

    quarters = ["Q1", "Q2", "Q3", "Q4"]

    for year in years:
        for q in quarters:
            ascii_dir = os.path.join(INPUT_ROOT, year, q, "ascii")
            if not os.path.isdir(ascii_dir):
                continue

            # 只取 ascii 目录里的 .txt
            for txt_path in glob.glob(os.path.join(ascii_dir, "*.txt")):
                fname = os.path.basename(txt_path)
                stem, _ = os.path.splitext(fname)

                # 过滤：只处理那 8 张表
                prefix = stem[:4].upper()
                if prefix not in TABLE_PREFIXES:
                    continue

                out_dir = os.path.join(OUTPUT_ROOT, year, q)
                out_path = os.path.join(out_dir, f"{stem}.csv")

                tasks.append({
                    "year": year,
                    "quarter": q,
                    "stem": stem,
                    "input_path": txt_path,
                    "output_path": out_path,
                })

    main_logger.info(f"Discovered years: {len(years)} -> {years[:5]}{'...' if len(years) > 5 else ''}")
    main_logger.info(f"Discovered tasks: {len(tasks)}")
    return tasks


# =========================================================
# 8) Worker：进程级日志隔离 + settings
# =========================================================

WORKER_LOGGER = None
WORKER_SETTINGS = None

def worker_init(settings: dict):
    global WORKER_LOGGER, WORKER_SETTINGS
    WORKER_SETTINGS = settings

    proc_name = current_process().name
    logger = logging.getLogger(proc_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

    # 每个进程一个日志文件（避免混乱）
    log_path = os.path.join(settings["run_dir"], f"{proc_name}.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    WORKER_LOGGER = logger
    WORKER_LOGGER.info("Worker initialized.")


# =========================================================
# 9) 多进程调用函数：带重试
# =========================================================

def convert_task_with_retry(task: dict) -> dict:
    logger = WORKER_LOGGER
    s = WORKER_SETTINGS

    year = task["year"]
    q = task["quarter"]
    stem = task["stem"]
    input_path = task["input_path"]
    out_path = task["output_path"]

    result = {
        "year": year,
        "quarter": q,
        "file": stem,
        "input_path": input_path,
        "output_path": out_path,
        "status": "FAIL",
        "reason": "",
        "attempts": 0,
        "rows": 0,
        "cols": 0,
        "seconds": 0.0,
        "mode": "",
    }

    # 跳过已存在输出
    if s["skip_existing"] and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        result["status"] = "SKIP"
        result["reason"] = "OUTPUT_EXISTS"
        return result

    ok, reason = basic_file_validate(input_path)
    if not ok:
        result["reason"] = reason
        logger.error(f"[{year}/{q}] Precheck failed: {stem} | reason={reason} | path={input_path}")
        return result
    if reason == "NO_DELIM_IN_HEAD_WARN":
        logger.warning(f"[{year}/{q}] Precheck warn(no '$' in head): {stem} | path={input_path}")

    size_mb = os.path.getsize(input_path) / (1024 * 1024)
    use_chunk = size_mb >= s["chunk_threshold_mb"]

    for attempt in range(1, s["max_retries"] + 1):
        result["attempts"] = attempt
        start = time.time()

        try:
            logger.info(f"[{year}/{q}] Start {stem} | attempt={attempt} | {size_mb:.1f}MB | mode={'chunk' if use_chunk else 'full'}")

            if use_chunk:
                chunks = read_faers_chunks(input_path)
                rows, cols = atomic_write_csv_chunks(chunks, out_path, logger)
                result["rows"] = rows
                result["cols"] = cols
                result["mode"] = "chunk"
            else:
                df = read_faers_full(input_path)
                df = clean_df(df)
                result["rows"] = len(df)
                result["cols"] = df.shape[1]
                result["mode"] = "full"
                atomic_write_csv(df, out_path)

            result["status"] = "OK"
            result["reason"] = "OK"
            result["seconds"] = round(time.time() - start, 3)

            logger.info(f"[{year}/{q}] OK {stem} | rows={result['rows']} cols={result['cols']} sec={result['seconds']}")
            return result

        except MemoryError:
            result["seconds"] = round(time.time() - start, 3)
            logger.error(f"[{year}/{q}] MemoryError {stem} attempt={attempt}")
            logger.error(traceback.format_exc())
            gc.collect()

        except Exception:
            result["seconds"] = round(time.time() - start, 3)
            logger.error(f"[{year}/{q}] Exception {stem} attempt={attempt}")
            logger.error(traceback.format_exc())

        # 清理 tmp
        try:
            tmp_path = out_path + ".tmp"
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            logger.warning(f"[{year}/{q}] Could not remove tmp: {stem}")

        if attempt < s["max_retries"]:
            backoff = s["base_backoff_sec"] * attempt
            logger.info(f"[{year}/{q}] Retry {stem} after {backoff}s...")
            time.sleep(backoff)
        else:
            result["status"] = "FAIL"
            result["reason"] = "FAILED_AFTER_RETRIES"
            logger.error(f"[{year}/{q}] FAIL {stem} after {attempt} attempts")
            return result


# =========================================================
# 10) 主流程：发现任务 -> 多进程 -> 汇总 -> 失败清单
# =========================================================

def main():
    main_logger = build_main_logger()
    main_logger.info("===== FAERS DECODE (ALL YEARS/QUARTERS) START =====")
    main_logger.info(f"INPUT_ROOT : {INPUT_ROOT}")
    main_logger.info(f"OUTPUT_ROOT: {OUTPUT_ROOT}")
    main_logger.info(f"RUN_DIR    : {RUN_DIR}")

    tasks = discover_tasks(main_logger)
    total = len(tasks)
    if total == 0:
        main_logger.warning("No tasks discovered. Check directory structure and file extensions.")
        return

    cpu = os.cpu_count() or 2
    proc_num = PROCESS_NUM if PROCESS_NUM is not None else min(cpu, total)
    proc_num = max(1, int(proc_num))

    main_logger.info(f"CPU_COUNT={cpu} | PROCESS_NUM={proc_num} | MAX_RETRIES={MAX_RETRIES} | SKIP_EXISTING={SKIP_EXISTING}")
    main_logger.info(f"CHUNK_THRESHOLD_MB={CHUNK_THRESHOLD_MB} | CHUNK_ROWS={CHUNK_ROWS}")

    settings = {
        "run_dir": RUN_DIR,
        "max_retries": MAX_RETRIES,
        "base_backoff_sec": BASE_BACKOFF_SEC,
        "chunk_threshold_mb": CHUNK_THRESHOLD_MB,
        "skip_existing": SKIP_EXISTING,
    }

    ctx = get_context("spawn")  # Windows 友好
    start_all = time.time()
    results = []

    with ctx.Pool(processes=proc_num, initializer=worker_init, initargs=(settings,)) as pool:
        completed = 0
        for res in pool.imap_unordered(convert_task_with_retry, tasks):
            results.append(res)
            completed += 1
            main_logger.info(
                f"[{completed}/{total}] {res['status']} {res['year']}/{res['quarter']} {res['file']} "
                f"| rows={res.get('rows')} cols={res.get('cols')} sec={res.get('seconds')} mode={res.get('mode')} reason={res.get('reason')}"
            )

    elapsed = round(time.time() - start_all, 3)

    ok_list = [r for r in results if r["status"] == "OK"]
    skip_list = [r for r in results if r["status"] == "SKIP"]
    fail_list = [r for r in results if r["status"] == "FAIL"]

    main_logger.info("===== SUMMARY =====")
    main_logger.info(f"Total   : {total}")
    main_logger.info(f"OK      : {len(ok_list)}")
    main_logger.info(f"SKIP    : {len(skip_list)}")
    main_logger.info(f"FAIL    : {len(fail_list)}")
    main_logger.info(f"Elapsed : {elapsed} sec")

    failed_txt = os.path.join(RUN_DIR, f"failed_files_{RUN_TS}.txt")
    report_json = os.path.join(RUN_DIR, f"report_{RUN_TS}.json")

    if fail_list:
        with open(failed_txt, "w", encoding="utf-8") as f:
            for r in sorted(fail_list, key=lambda x: (x["year"], x["quarter"], x["file"])):
                f.write(f"{r['year']}\t{r['quarter']}\t{r['file']}\t{r.get('reason','')}\t{r.get('input_path','')}\n")
        main_logger.warning(f"Failed list saved: {failed_txt}")
    else:
        main_logger.info("No failed files.")

    report = {
        "run_ts": RUN_TS,
        "input_root": INPUT_ROOT,
        "output_root": OUTPUT_ROOT,
        "process_num": proc_num,
        "max_retries": MAX_RETRIES,
        "skip_existing": SKIP_EXISTING,
        "chunk_threshold_mb": CHUNK_THRESHOLD_MB,
        "chunk_rows": CHUNK_ROWS,
        "elapsed_sec": elapsed,
        "counts": {
            "total": total,
            "ok": len(ok_list),
            "skip": len(skip_list),
            "fail": len(fail_list),
        },
        "results": sorted(results, key=lambda x: (x["year"], x["quarter"], x["file"])),
    }

    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    main_logger.info(f"Report saved: {report_json}")
    main_logger.info(f"Process logs in: {RUN_DIR}")
    main_logger.info("===== FAERS DECODE END =====")


if __name__ == "__main__":
    main()
