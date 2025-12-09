# coding=utf-8
"""
말/기수 상관전적 수집 스크립트.
"""

import logging
import random
import time

import pandas as pd
from autils.db.mysql_utils import MySQL


def collect_relation_record(location: int, bas_dt: str, rc_no: int, try_cnt: int = 1) -> pd.DataFrame:
    if try_cnt > 3:
        logging.info("데이터 수집 시도 횟수가 3회를 초과했습니다.")
        return pd.DataFrame()

    if try_cnt > 1:
        time.sleep(random.randint(3, 6))

    url = (
        "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoRelationRecord.do?"
        f"meet={location}&rcNo={rc_no}&rcDate={bas_dt}"
    )
    logging.info("%s data collect start", url)

    try:
        tables = pd.read_html(url, encoding="euc-kr")
    except Exception as e:
        logging.error("데이터 수집 중 오류 발생: %s", e)
        return collect_relation_record(location, bas_dt, rc_no, try_cnt + 1)

    df = pd.DataFrame()
    for t in tables:
        cand = t.copy()
        if isinstance(cand.columns, pd.MultiIndex):
            cand.columns = [f"{c[0]}_{c[1]}" if c[1] else c[0] for c in cand.columns]
        cand.columns = [str(c).replace(" ", "").replace("\n", "") for c in cand.columns]
        if any(k in " ".join(cand.columns) for k in ["마번", "마명", "번호"]):
            df = cand
            break

    if df.empty:
        logging.info("%s, %s, %s 데이터는 없습니다.", bas_dt, location, rc_no)
        return pd.DataFrame()

    df["BAS_DT"] = bas_dt
    df["rc_no"] = rc_no
    df["meet"] = location
    return df.fillna("")


def _ensure_table(ms: MySQL, table_name: str, columns: pd.Index) -> None:
    col_defs = ", ".join(f"`{c}` VARCHAR(255)" for c in columns)
    pk_candidates = [c for c in ["BAS_DT", "rc_no", "meet", "마번"] if c in columns]
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs}"
    if pk_candidates:
        pk = ", ".join(f"`{c}`" for c in pk_candidates)
        create_sql += f", PRIMARY KEY ({pk})"
    create_sql += ") CHARACTER SET utf8mb4"
    ms.execute(create_sql)


def insert_table(df: pd.DataFrame, table_name: str = "hr_relation_record") -> int:
    if df is None or df.empty:
        logging.info("적재할 데이터가 없습니다.")
        return 0

    df = df.astype(str)
    columns = df.columns.tolist()
    rows = df.values.tolist()

    for row in rows:
        for idx, value in enumerate(row):
            if isinstance(value, str) and len(value) > 200:
                row[idx] = value[:200]

    try:
        with MySQL() as ms:
            _ensure_table(ms, table_name, df.columns)
            col_list = ", ".join(f"`{c}`" for c in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"REPLACE INTO {table_name} ({col_list}) VALUES ({placeholders})"
            ms.insertmany(insert_sql, rows)
    except Exception as e:
        logging.error("DB 적재 중 오류 발생: %s", e)
        return 0
    return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    sample_dt = "20251122"
    df_relation = collect_relation_record(1, sample_dt, 1, 1)
    print(df_relation.head())
    insert_table(df_relation)
