# coding=utf-8
"""
지정한 기간(from_dt~to_dt) 동안 서울(또는 meet 지정) 경주 정보를 수집한다.
1) 목록 페이지에서 BAS_DT, rc_no를 추출해 DB에 저장
2) 추출한 일정으로 step01~step12 스크립트를 순차 실행해 각 테이블 적재
"""

import logging
import random
import time
from typing import Dict, List, Set, Tuple

import pandas as pd
from autils.db.mysql_utils import MySQL

from step01_collect_race_result import collect_race_result, insert_table as insert_hr_result
from step02_get_entry import collect_entry as collect_entry_info, insert_table as insert_hr_entry
from step03_get_medical import collect_entry as collect_medical, insert_table as insert_hr_medical
from step04_get_weight import collect_weight, insert_table as insert_hr_weight
from step05_get_record import collect_record, insert_table as insert_hr_record
from step06_get_distance_record import collect_distance_record, insert_table as insert_hr_distance_record
from step07_get_match_record import collect_match_record, insert_table as insert_hr_match_record
from step08_get_train_state import collect_train_state, insert_table as insert_hr_train_state
from step09_get_relation_record import collect_relation_record, insert_table as insert_hr_relation_record
from step10_get_recent10 import collect_recent10, insert_table as insert_hr_recent10
from step11_get_stewards_report import collect_stewards_report, insert_table as insert_hr_stewards_report
from step12_get_starting_train import collect_starting_train, insert_table as insert_hr_starting_train


LIST_URL = (
    "https://race.kra.co.kr/raceScore/ScoretableScoreList.do"
    "?nextFlag=false&meet={meet}&realRcDate=&realRcNo=&Act=04&Sub=1&fromDate={from_dt}&toDate={to_dt}&pageIndex={page}"
)

def get_race_dt(from_dt, to_dt, meet):

    if meet==1:
        # 날짜 범위 생성
        dates = pd.date_range(from_dt, to_dt, freq='D')

        # 금(Friday=4), 토(Saturday=5)에 해당하는 날짜만 필터링
        weekend_dates = dates[dates.weekday.isin([4, 5])]

    return weekend_dates


def fetch_schedule(from_dt: str, to_dt: str, meet: int) -> pd.DataFrame:
    """
    목록 페이지를 순회하며 (BAS_DT, rc_no, meet)를 수집한다.
    """
    seen: Set[Tuple[str, int, int]] = set()    
    page = 1
    df_lst = list()

    while True:
        url = LIST_URL.format(meet=meet, from_dt=from_dt, to_dt=to_dt, page=page)
        logging.info("목록 페이지 수집: %s", url)
        try:
            tables = pd.read_html(url, encoding="euc-kr")
        except Exception as e:
            logging.warning("페이지 로드 실패(종료): %s", e)
            break

        if tables[0].iloc[0]['순서']=="자료가 없습니다.":
            logging.info("더 이상 테이블이 없어 종료합니다. page=%s", page)
            break

        target = pd.DataFrame()
        for t in tables:
            cols = [str(c).replace(" ", "").replace("\n", "") for c in t.columns]
            if any(key in " ".join(cols) for key in ["경기일자", "경주번호", "경주일자", "경주일"]):
                target = t.copy()
                target.columns = cols
                break

        # 컬럼 이름 후보
        date_col = next((c for c in target.columns if "경기일" in c or "경주일" in c), None)
        # rc_col = next((c for c in target.columns if "경주번호" in c or "경주" in c), None)
        rc_col = "경주"
        if not date_col or not rc_col:
            logging.info("목록 테이블에서 컬럼을 찾지 못했습니다. page=%s", page)
            break

        target['경주일자'] = target['경주일자'].str.split("(").str[0].str.replace("/","").str.strip()
        target['경주'] = target['경주'].str.split(" ")
        target = target.set_index("경주일자")
        target = target.explode('경주').reset_index(drop=False)
        target = target.query("경주!=''")
        target['경주'] = target['경주'].astype(int)
        target = target.rename(columns={'경주일자':'BAS_DT', '경주':"rc_no"})
        target['meet'] = meet
        target = target[['BAS_DT','rc_no','meet']]
        df_lst.append(target)
        
        # 다음 페이지를 시도
        page += 1
        time.sleep(random.uniform(0.5, 1.2))

    df = pd.concat(df_lst, axis=0)

    return df


def _ensure_schedule_table(ms: MySQL, table_name: str = "hr_race_schedule") -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
        BAS_DT VARCHAR(10),
        meet VARCHAR(5),
        rc_no VARCHAR(5),
        PRIMARY KEY (BAS_DT, meet, rc_no)
    ) CHARACTER SET utf8mb4
    """
    ms.execute(sql)


def save_schedule(df: pd.DataFrame, table_name: str = "hr_race_schedule") -> None:
    if df is None or df.empty:
        logging.info("저장할 일정 데이터가 없습니다.")
        return
    with MySQL() as ms:
        _ensure_schedule_table(ms, table_name)
        df = df.astype(str)
        cols = df.columns.tolist()
        col_list = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"REPLACE INTO {table_name} ({col_list}) VALUES ({placeholders})"
        print(sql)
        print(col_list)
        print(placeholders)
        ms.insertmany(sql, df.values.tolist())


def run_collectors(bas_dt: str, meet: int, rc_no: int) -> None:
    """
    각 수집 스크립트를 순차 실행하여 DB에 적재.
    """
    logging.info("=== 수집 시작: BAS_DT=%s, meet=%s, rc_no=%s ===", bas_dt, meet, rc_no)

    # step01: 경주 결과
    df_result = collect_race_result(meet, bas_dt, rc_no, 1)
    if not df_result.empty:
        df_result = df_result[[
        "BAS_DT", "meet", "rc_no",
        '순위', '마번', '마명', '산지', '성별', '연령', '중량', '레이팅',
        '기수명', '조교사명', '마주명', '도착차', '마체중', '단승', '연승',
        '복승', '쌍승', '복연승', '삼복승', '삼쌍승', '장구현황', 'S1F_G1F',
        'S_1F', '1코너', '2코너', '3코너', 'G_3F', '4코너', 'G_1F', '3F_G',
        '1F_G', '10_8F', '8_6F', '6_4F', '4_2F', '2F_G', 'day', 'day_th',
        'weather', 'race_st', 'race_time', 'race_infor1', 'distance',
        'race_infor2', 'race_infor3', 'race_infor4', 'race_infor5'
        ]]
        ms = MySQL()
        ms.insert_df_to_table(df_result, "hr_result")

    # step02: 출마표
    print("step02 출마표 수집")
    df_entry = collect_entry_info(meet, bas_dt, rc_no, 1)
    if not df_entry.empty:
        insert_hr_entry(df_entry, "hr_entry")

    # step03: 장구/메디컬
    print("step03 장구/메디컬 수집")
    df_medical = collect_medical(meet, bas_dt, rc_no, 1)
    if not df_medical.empty:
        insert_hr_medical(df_medical, "hr_medical")

    # step04~12
    collectors = [
        (collect_weight, insert_hr_weight),
        (collect_record, insert_hr_record),
        (collect_distance_record, insert_hr_distance_record),
        (collect_match_record, insert_hr_match_record),
        (collect_train_state, insert_hr_train_state),
        (collect_relation_record, insert_hr_relation_record),
        (collect_recent10, insert_hr_recent10),
        (collect_stewards_report, insert_hr_stewards_report),
        (collect_starting_train, insert_hr_starting_train),
    ]

    for collect_fn, insert_fn in collectors:
        print("*"*5, collect_fn.__name__,"*"*5)
        try:
            df = collect_fn(meet, bas_dt, rc_no, 1)
            if df is not None and not df.empty:
                insert_fn(df)
        except Exception as e:
            logging.error("수집/적재 오류 (%s): %s", collect_fn.__name__, e)
            continue


def collect_range(from_dt: str, to_dt: str, meet: int = 1) -> None:
    """
    기간 내 경주 일정 수집 후 모든 세부 데이터를 적재.
    """
    schedule_df = fetch_schedule(from_dt, to_dt, meet)
    logging.info("수집된 일정 %d건", len(schedule_df))
    if len(schedule_df)==0:
        return
    
    save_schedule(schedule_df)

    for _, row in schedule_df.sort_values(["BAS_DT", "rc_no"]).iterrows():
        run_collectors(row["BAS_DT"], int(row["meet"]), int(row["rc_no"]))
        time.sleep(random.uniform(1.0, 2.0))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 예시 실행: 2025-01-01 ~ 2025-01-07, 서울(meet=1)
    collect_range("20251129", "20251129", meet=1)
