# coding=utf-8
"""
출마표/장구 현황 수집 스크립트.
step01_collect_race_result.py와 동일한 흐름으로 작성되었으며,
- URL에서 데이터를 pandas로 읽어온 뒤
- 전처리 후 MySQL 테이블에 적재한다.
"""

import logging
import random
import time

import pandas as pd
from autils.db.mysql_utils import MySQL


def collect_entry(location: int, bas_dt: str, rc_no: int, try_cnt: int = 1) -> pd.DataFrame:
    """
    출마표/장구 현황을 수집한다.
    :param location: 1(서울), 3(부산)
    :param bas_dt: 경주일자 YYYYMMDD
    :param rc_no: 경주 번호
    :param try_cnt: 재시도 횟수
    :return: 수집된 DataFrame, 실패 시 빈 DataFrame 반환
    """
    if try_cnt > 3:
        logging.info("데이터 수집 시도 횟수가 3회를 초과했습니다.")
        return pd.DataFrame()

    if try_cnt > 1:
        time.sleep(random.randint(3, 6))

    url = (
        "http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?"
        f"Act=02&Sub=1&meet={location}&rcNo={rc_no}&rcDate={bas_dt}"
    )
    logging.info("%s data collect start", url)

    try:
        tables = pd.read_html(url, encoding="euc-kr")
    except Exception as e:
        logging.error("데이터 수집 중 오류 발생: %s", e)
        return collect_entry(location, bas_dt, rc_no, try_cnt + 1)

    entry_df = pd.DataFrame()

    # 장구 현황 테이블 추출: 마번/마명/번호 등의 컬럼이 있는 첫 번째 테이블을 사용
    for t in tables:
        df_candidate = t.copy()
        if isinstance(df_candidate.columns, pd.MultiIndex):
            df_candidate.columns = [
                f"{c[0]}_{c[1]}" if c[1] else c[0] for c in df_candidate.columns
            ]

        df_candidate.columns = [
            str(c).replace(" ", "").replace("\n", "") for c in df_candidate.columns
        ]

        col_str = " ".join(df_candidate.columns)
        if any(key in col_str for key in ["마번", "마명", "번호"]):
            entry_df = df_candidate
            break

    if entry_df.empty:
        logging.info("%s, %s, %s 데이터는 없습니다.", bas_dt, location, rc_no)
        return pd.DataFrame()

    entry_df["BAS_DT"] = bas_dt
    entry_df["rc_no"] = rc_no
    entry_df["meet"] = location

    return entry_df.fillna("")


def _ensure_table(ms: MySQL, table_name: str, columns: pd.Index) -> None:
    """필요한 테이블이 없으면 생성한다."""
    col_defs = ", ".join(f"`{c}` VARCHAR(255)" for c in columns)
    pk_candidates = [c for c in ["BAS_DT", "rc_no", "meet", "마번"] if c in columns]

    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs}"
    if pk_candidates:
        pk = ", ".join(f"`{c}`" for c in pk_candidates)
        create_sql += f", PRIMARY KEY ({pk})"
    create_sql += ") CHARACTER SET utf8mb4"

    ms.execute(create_sql)


def insert_table(entry_df: pd.DataFrame, table_name: str = "hr_medical") -> int:
    """
    수집된 DataFrame을 MySQL에 적재한다.
    :param entry_df: 수집 데이터
    :param table_name: 대상 테이블명
    :return: 성공 시 1, 실패 시 0
    """
    if entry_df is None or entry_df.empty:
        logging.info("적재할 데이터가 없습니다.")
        return 0

    entry_df = entry_df.astype(str)
    columns = entry_df.columns.tolist()
    rows = entry_df.values.tolist()

    # 값 길이 제한 (너무 긴 문자열은 잘라서 저장)
    for row in rows:
        for idx, value in enumerate(row):
            if isinstance(value, str) and len(value) > 200:
                row[idx] = value[:200]

    try:
        with MySQL() as ms:
            _ensure_table(ms, table_name, entry_df.columns)

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

    # 사용 예시
    sample_date = "20251122"
    sample_location = 1  # 서울
    sample_rc_no = 1

    df_entry = collect_entry(sample_location, sample_date, sample_rc_no, 1)
    print(df_entry.head())

    # DB 적재
    insert_table(df_entry, "hr_medical")
