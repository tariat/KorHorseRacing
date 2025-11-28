# coding=utf-8
"""
모든 수집 테이블을 BAS_DT/meet/rc_no 기준으로 결합해
- pandas DataFrame 형태를 반환하고
- AI가 읽기 좋은 JSON 레이아웃으로 저장한다.
"""

import json
import logging
import os
from typing import Dict, List

import pandas as pd
from autils.db.mysql_utils import MySQL


TABLES = {
    "result": "hr_result",
    "entry": "hr_entry",
    "medical": "hr_medical",
    "weight": "hr_weight",
    "record": "hr_record",
    "distance_record": "hr_distance_record",
    "match_record": "hr_match_record",
    "train_state": "hr_train_state",
    "relation_record": "hr_relation_record",
    "recent10": "hr_recent10",
    "stewards_report": "hr_stewards_report",
    "starting_train": "hr_starting_train",
}

KEY_COL_CANDIDATES = ["마번", "번호"]
COMMON_KEYS = ["BAS_DT", "meet", "rc_no"]


def _load_table(ms: MySQL, table_name: str, bas_dt: str, meet: int, rc_no: int) -> pd.DataFrame:
    sql = f"SELECT * FROM {table_name} WHERE BAS_DT=%s AND meet=%s AND rc_no=%s"
    try:
        df = ms.get(sql, [bas_dt, meet, rc_no])
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    key_col = next((c for c in KEY_COL_CANDIDATES if c in df.columns), None)
    if key_col is None:
        return pd.DataFrame()

    # 키 컬럼명을 통일
    df = df.rename(columns={key_col: "마번"})
    df["마번"] = df["마번"].astype(str)
    df = df.drop_duplicates(subset=["마번"])
    return df


def _prefix_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    rename_map = {c: f"{prefix}_{c}" for c in df.columns if c not in COMMON_KEYS + ["마번", "마명"]}
    return df.rename(columns=rename_map)


def combine_to_dataframe(bas_dt: str, meet: int, rc_no: int) -> pd.DataFrame:
    """
    BAS_DT/meet/rc_no별 모든 테이블을 결합한 단일 DataFrame을 반환한다.
    """
    with MySQL() as ms:
        loaded: Dict[str, pd.DataFrame] = {
            name: _load_table(ms, tbl, bas_dt, meet, rc_no) for name, tbl in TABLES.items()
        }

    # 기준 DF: result가 있으면 사용, 없으면 entry 사용
    base = loaded.get("result")
    if base is None or base.empty:
        base = loaded.get("entry", pd.DataFrame())

    if base.empty:
        return pd.DataFrame()

    base = base.rename(columns={"마번": "마번"})  # ensure exists
    merged = base[["BAS_DT", "meet", "rc_no", "마번"] + [c for c in base.columns if c not in COMMON_KEYS + ["마번"]]]

    for name, df in loaded.items():
        if df is None or df.empty or name == "result":
            continue
        df_prefixed = _prefix_columns(df, name)
        merged = pd.merge(merged, df_prefixed, how="left", on=["BAS_DT", "meet", "rc_no", "마번"])

    # NaN 처리
    return merged.fillna("")


def _row_to_nested(row: Dict, table_prefixes: List[str]) -> Dict:
    horse = {
        "horse_no": row.get("마번", ""),
        "horse_name": row.get("마명", ""),
        "jockey": row.get("기수명", ""),
        "trainer": row.get("조교사명", ""),
        "owner": row.get("마주명", ""),
        "tables": {},
    }

    # 기본 result 정보
    horse["tables"]["result"] = {
        k: v for k, v in row.items() if not any(k.startswith(prefix + "_") for prefix in table_prefixes)
    }

    # 테이블별 블록
    for prefix in table_prefixes:
        prefixed_items = {k.replace(prefix + "_", "", 1): v for k, v in row.items() if k.startswith(prefix + "_")}
        if prefixed_items:
            horse["tables"][prefix] = prefixed_items

    return horse


def dataframe_to_json(df: pd.DataFrame, bas_dt: str, meet: int, rc_no: int, out_path: str) -> Dict:
    """
    DataFrame을 AI 친화적인 JSON 레이아웃으로 변환하고 파일로 저장한다.
    """
    if df is None or df.empty:
        payload = {"metadata": {"BAS_DT": bas_dt, "meet": meet, "rc_no": rc_no}, "horses": []}
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return payload

    table_prefixes = [name for name in TABLES.keys() if name != "result"]
    horses = [_row_to_nested(row, table_prefixes) for row in df.to_dict(orient="records")]

    payload = {
        "metadata": {
            "BAS_DT": bas_dt,
            "meet": meet,
            "rc_no": rc_no,
            "horse_count": len(horses),
            "tables": list(TABLES.keys()),
        },
        "horses": horses,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    sample_bas_dt = "20251122"
    sample_meet = 1  # 서울
    sample_rc_no = 1

    df_all = combine_to_dataframe(sample_bas_dt, sample_meet, sample_rc_no)
    print(df_all.head())

    out_file = f"./output/combined_{sample_bas_dt}_{sample_meet}_{sample_rc_no}.json"
    dataframe_to_json(df_all, sample_bas_dt, sample_meet, sample_rc_no, out_file)
    print(f"JSON saved to {out_file}")
