# coding=utf-8
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pymysql
import time
import random
import re

"""
경주 성적: url0="http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet=1&realRcDate=20170212&realRcNo=1"
폐출혈: url1="http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?Act=02&Sub=1&meet=1&rcNo=3&rcDate="+date
원래URL: http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=1%B5%EE%B1%DE&csdkfjsf9ZVx11ja8a=skd8ahd8sh1sd1s
1등급: http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=1%B5%EE%B1%DE
국산마3등급: http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=%B1%B93
4등급: %B1%B94
미검: /racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=%BF%DC%B9%CC%B0%CB&csdkfjsf9ZVx11ja8a=skd8ahd8sh1sd1s
"""

db_conf = {
"host": "127.0.0.1",
"user": "test",
"password": "test11",
"database": "flask1",
}

def create_table(db_conf):
    """
    데이터를 저장할 mysql테이블을 만듭니다.
    :param db_conf: DB접속정보
    :return:
    """

    con=pymysql.connect(**db_conf)
    cur=con.cursor()
    
    # 기존 테이블 삭제 후 재생성 (스키마 변경을 위해)
    cur.execute("DROP TABLE IF EXISTS hr_result")
    
    cur.execute("""
        CREATE TABLE hr_result  
        (
            순위 VARCHAR(10),
            마번 VARCHAR(10),
            마명 VARCHAR(30),
            산지 VARCHAR(20),
            성별 VARCHAR(10),
            연령 VARCHAR(5),
            중량 VARCHAR(5),
            레이팅 VARCHAR(5),
            기수명 VARCHAR(30),
            조교사명 VARCHAR(30),
            마주명 VARCHAR(50),
            도착차 VARCHAR(50),
            마체중 VARCHAR(10),
            단승 VARCHAR(10),
            연승 VARCHAR(10),
            복승 VARCHAR(10),
            쌍승 VARCHAR(10),
            복연승 VARCHAR(10),
            삼복승 VARCHAR(10),
            삼쌍승 VARCHAR(10),
            장구현황 VARCHAR(100),
            S1F_G1F VARCHAR(100),
            S_1F VARCHAR(50),
            1코너 VARCHAR(50),
            2코너 VARCHAR(50),
            3코너 VARCHAR(50),
            G_3F VARCHAR(50),
            4코너 VARCHAR(50),
            G_1F VARCHAR(50),
            3F_G VARCHAR(50),
            1F_G VARCHAR(50),
            10_8F VARCHAR(50),
            8_6F VARCHAR(50),
            6_4F VARCHAR(50),
            4_2F VARCHAR(50),
            2F_G VARCHAR(50),
            day VARCHAR(50),
            day_th VARCHAR(20),
            weather VARCHAR(20),
            race_st VARCHAR(10),
            race_time VARCHAR(10),
            race_infor1 VARCHAR(10),
            distance VARCHAR(10),
            race_infor2 VARCHAR(10),
            race_infor3 VARCHAR(30),
            race_infor4 VARCHAR(10),
            race_infor5 VARCHAR(10),
            primary key (day,day_th,마명)
        )
        """)
    con.commit()
    con.close()

    return 1

def collect_race(location,date,rc_no,try_cnt):
    """
    경기결과 수집 (pandas read_html 방식으로 업데이트)
    :param location: 서울1, 부산3
    :param date: 날짜
    :param rc_no: 경기번호
    :return: 1: 수집결과, 정상수집이 안 되면 행이 0인 데이터프레임을 return
    """
    if  try_cnt<3:
        time.sleep(random.randint(3,6))
        pass
    else:
        logging.info("데이터 수집 시도 횟수가 3회 초과하였습니다.")
        return pd.DataFrame()

    try:
        url="http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet={}&realRcDate={}&realRcNo={}".format(location,date,rc_no)
        logging.info("{} data collect start".format(url))

        # pandas read_html로 테이블 추출 (인코딩 지정)
        tables = pd.read_html(url, encoding='euc-kr')
        
        if len(tables) < 4:
            logging.info("{}, {}, {} 데이터는 없습니다.".format(date,location,rc_no))
            return pd.DataFrame()

        # 경주 정보 추출 (테이블 0)
        race_info = tables[0]
        
        # 경주 결과 데이터 (테이블 2 - 주요 경주 결과)
        hr_stat = tables[2].copy()
        
        # 구간별 기록 데이터 (테이블 3)
        record_detail = tables[3].copy()
        
        if len(hr_stat) == 0:
            logging.info("{}, {}, {} 경주 결과 데이터는 없습니다.".format(date,location,rc_no))
            return pd.DataFrame()

        # 컬럼명 정리
        if record_detail.columns.nlevels > 1:
            # MultiIndex 컬럼인 경우 처리
            record_detail.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in record_detail.columns]
        
        # 기본 컬럼명 매핑
        col_rename_map = {
            'S1F-1C-2C-3C-G3F-4C-G1F': 'S1F_G1F',
            'S1F-1C-2C-3C-4C-G1F': 'S1F_G1F',
            'S1F지점': 'S_1F',
            'S-1F': 'S_1F',
            '1코너지점': '1코너',
            '2코너지점': '2코너', 
            '3코너지점': '3코너',
            '4코너지점': '4코너',
            'G1F지점': 'G_1F',
            'G-1F': 'G_1F',
            'G3F지점': 'G_3F',
            'G-3F': 'G_3F',
            '10-8F': '10_8F',
            '8-6F': '8_6F',
            '6-4F': '6_4F',
            '4-2F': '4_2F',
            '2F-G': '2F_G',
            '3F-G': '3F_G',
            '1F-G': '1F_G'
        }
        
        # 컬럼명 변경
        for old_col in record_detail.columns:
            for old_name, new_name in col_rename_map.items():
                if old_name in old_col:
                    record_detail = record_detail.rename(columns={old_col: new_name})
                    break

        # 경주 정보 추출 개선 (실제 웹페이지 구조에 맞게 수정)
        try:
            # 첫 번째 행에서 경주 기본 정보 추출
            day = str(race_info.iloc[0, 0]).strip() if len(race_info) > 0 and race_info.shape[1] > 0 else ""
            day_th = str(race_info.iloc[0, 4]).strip() if len(race_info) > 0 and race_info.shape[1] > 4 else ""  # "제 90일"
            weather = str(race_info.iloc[0, 5]).strip() if len(race_info) > 0 and race_info.shape[1] > 5 else ""  # "맑음"
            race_st = str(race_info.iloc[0, 6]).strip() if len(race_info) > 0 and race_info.shape[1] > 6 else ""  # "포화"
            race_time = str(race_info.iloc[0, 8]).strip() if len(race_info) > 0 and race_info.shape[1] > 8 else ""  # "10:35"
            
            # 두 번째 행에서 경주 상세 정보 추출
            race_infor1 = str(race_info.iloc[1, 0]).strip() if len(race_info) > 1 and race_info.shape[1] > 0 else ""  # "국6등급"
            distance = str(race_info.iloc[1, 1]).strip() if len(race_info) > 1 and race_info.shape[1] > 1 else ""  # "1200M"
            race_infor2 = str(race_info.iloc[1, 2]).strip() if len(race_info) > 1 and race_info.shape[1] > 2 else ""  # "별정A"
            race_infor3 = str(race_info.iloc[1, 3]).strip() if len(race_info) > 1 and race_info.shape[1] > 3 else ""  # "일반"
            race_infor4 = str(race_info.iloc[1, 6]).strip() if len(race_info) > 1 and race_info.shape[1] > 6 else ""  # "2세 오픈"
            race_infor5 = str(race_info.iloc[1, 8]).strip() if len(race_info) > 1 and race_info.shape[1] > 8 else ""  # "루키"
                
        except Exception as e:
            logging.warning(f"경주 정보 추출 중 오류: {e}")
            day = day_th = weather = race_st = race_time = ""
            race_infor1 = distance = race_infor2 = race_infor3 = race_infor4 = race_infor5 = ""

        # 배당 정보 추출 (테이블 5, 6 등에서)
        div = {}
        try:
            if len(tables) > 5:
                betting_tables = tables[5:8]  # 배당 관련 테이블들
                for betting_table in betting_tables:
                    for _, row in betting_table.iterrows():
                        for col in betting_table.columns:
                            cell_text = str(row[col])
                            if '복승식:' in cell_text:
                                import re
                                match = re.search(r'복승식:\s*[①-⑳]+\s*([\d,]+)', cell_text)
                                if match:
                                    div['복승식'] = match.group(1).replace(',', '')
                            elif '삼복승식:' in cell_text:
                                match = re.search(r'삼복승식:\s*[①-⑳]+\s*([\d,]+)', cell_text)
                                if match:
                                    div['삼복승식'] = match.group(1).replace(',', '')
                            elif '쌍승식:' in cell_text:
                                match = re.search(r'쌍승식:\s*[①-⑳]+\s*([\d,]+)', cell_text)
                                if match:
                                    div['쌍승식'] = match.group(1).replace(',', '')
                            elif '복연승식:' in cell_text:
                                match = re.search(r'복연승식:\s*[①-⑳]+\s*([\d,]+)', cell_text)
                                if match:
                                    div['복연승식'] = match.group(1).replace(',', '')
        except:
            pass

        # 데이터 병합 - 마번을 기준으로
        if '마번' in hr_stat.columns and '마번' in record_detail.columns:
            hr_1 = pd.merge(hr_stat, record_detail.drop(columns=['순위'], errors='ignore'), on="마번", how='left')
        else:
            hr_1 = hr_stat.copy()
            
        # 누락된 컬럼들 추가
        missing_cols = ['S1F_G1F', 'S_1F', '1코너', '2코너', '3코너', 'G_3F', '4코너', 'G_1F', 
                       '3F_G', '1F_G', '10_8F', '8_6F', '6_4F', '4_2F', '2F_G']
        
        for col in missing_cols:
            if col not in hr_1.columns:
                hr_1[col] = ""

        # 지역별 데이터 정리
        if location == 1:  # 서울
            hr_1["10_8F"] = ""
            hr_1["8_6F"] = ""
            hr_1["6_4F"] = ""
            hr_1["4_2F"] = ""
            hr_1["2F_G"] = ""
        elif location == 3:  # 부산
            hr_1["1코너"] = ""
            hr_1["2코너"] = ""
            hr_1["3코너"] = ""
            hr_1["G_3F"] = ""
            hr_1["4코너"] = ""
            hr_1["G_1F"] = ""

        # 경주 정보 추가
        hr_1["day"] = str(day).strip()
        hr_1["day_th"] = str(day_th).strip()
        hr_1["weather"] = str(weather).strip()
        hr_1["race_st"] = str(race_st).strip()
        hr_1["race_time"] = str(race_time).strip()
        hr_1["race_infor1"] = str(race_infor1).strip()
        hr_1["distance"] = str(distance).strip()
        hr_1["race_infor2"] = str(race_infor2).strip()
        hr_1["race_infor3"] = str(race_infor3).strip()
        hr_1["race_infor4"] = str(race_infor4).strip()
        hr_1["race_infor5"] = str(race_infor5).strip()
        
        # 배당 정보 추가
        hr_1["단승"] = div.get("단승식", "")
        hr_1["연승"] = div.get("연승식", "")
        hr_1["복승"] = div.get("복승식", "")
        hr_1["쌍승"] = div.get("쌍승식", "")
        hr_1["복연승"] = div.get("복연승식", "")
        hr_1["삼복승"] = div.get("삼복승식", "")
        hr_1["삼쌍승"] = div.get("삼쌍승식", "")

        return hr_1
        
    except Exception as e:
        logging.error(f"데이터 수집 중 오류 발생: {e}")
        if try_cnt < 3:
            hr_1 = collect_race(location, date, rc_no, try_cnt+1)
            return hr_1
        else:
            return pd.DataFrame()

def insert_table(hr_1, db_conf):

    # 실제 존재하는 컬럼만 선택하여 데이터베이스에 저장
    required_columns = ["순위", "마번", "마명", "산지", "성별", "연령", "중량", "레이팅", 
                       "기수명", "조교사명", "마주명", "도착차", "마체중", "단승", "연승", 
                       "복승", "쌍승", "복연승", "삼복승", "삼쌍승", "장구현황", "S1F_G1F", 
                       "S_1F", "1코너", "2코너", "3코너", "G_3F", "4코너", "G_1F", "3F_G", 
                       "1F_G", "10_8F", "8_6F", "6_4F", "4_2F", "2F_G", "day", "day_th", 
                       "weather", "race_st", "race_time", "race_infor1", "distance", 
                       "race_infor2", "race_infor3", "race_infor4", "race_infor5"]
    
    # 존재하지 않는 컬럼 처리
    for col in required_columns:
        if col not in hr_1.columns:
            hr_1[col] = ""
            
    # 지역별로 다른 컬럼 구조 처리
    available_columns = [col for col in required_columns if col in hr_1.columns]
    hr_2 = hr_1[available_columns]
    
    # 부산/서울 공통 47개 컬럼로 맞추기 (부산의 추가 컬럼 제거)
    if len(hr_2.columns) > 47:
        # 중복 컬럼이나 추가 컬럼 제거
        if '경주 기록' in hr_2.columns:
            hr_2 = hr_2.drop(columns=['경주 기록'])
        # 2F_G 중복 제거 (첫 번째 것만 유지)
        if hr_2.columns.tolist().count('2F_G') > 1:
            # 중복된 컬럼 중 뒤의 것들을 제거
            cols = hr_2.columns.tolist()
            first_2f_g_idx = cols.index('2F_G')
            duplicate_cols = []
            for i, col in enumerate(cols):
                if col == '2F_G' and i > first_2f_g_idx:
                    duplicate_cols.append(i)
            if duplicate_cols:
                hr_2 = hr_2.iloc[:, [i for i in range(len(cols)) if i not in duplicate_cols]]
    
    # 47개 컬럼이 되도록 부족한 컬럼 추가
    while len(hr_2.columns) < 47:
        missing_col = f"missing_col_{len(hr_2.columns)}"
        hr_2[missing_col] = ""
    
    # NaN 값을 빈 문자열로 변환
    hr_2 = hr_2.fillna("")
    
    # 데이터 타입을 문자열로 변환하여 MySQL 호환성 확보
    hr_2 = hr_2.astype(str)

    hr_2_list=hr_2.values.tolist()

    db_conf = {
        "host": "127.0.0.1",
        "user": "test",
        "password": "test11",
        "database": "flask1",
    }
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    # 데이터 길이 제한 (MySQL VARCHAR 크기에 맞게)
    for row in hr_2_list:
        for i, value in enumerate(row):
            if isinstance(value, str) and len(value) > 200:  # 너무 긴 문자열은 자르기
                row[i] = value[:200]
    
    cur.executemany("""REPLACE INTO hr_result VALUES
                       (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s
                       )""", hr_2_list)
    con.commit()
    con.close()

    return 1












    return 1

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 테이블 생성
    create_table(db_conf)

    # 최신 날짜로 테스트 (2024년 12월 1일)
    date = "20241201"

    # location 1은 서울, 3은 부산
    # location_list = [1, 3]
    location_list = [3]

    print(f"=== 경마 데이터 수집 시작 ===")
    print(f"수집 날짜: {date}")
    
    for l in location_list:
        location_name = "서울" if l == 1 else "부산"
        print(f"\n--- {location_name} 경마장 데이터 수집 ---")
        
        for rc_no in range(1, 20):
            try:
                hr_1 = collect_race(l, date, rc_no, 1)
                
                if len(hr_1) == 0:
                    print(f"{location_name} {rc_no}번 경주: 데이터 없음 (경주 종료)")
                    break
                else:
                    print(f"✅ {location_name} {rc_no}번 경주: {len(hr_1)}마리 데이터 수집 성공")
                    
                    # 데이터베이스에 저장
                    insert_table(hr_1, db_conf)
                    print(f"   데이터베이스 저장 완료")
                    
            except Exception as e:
                logging.error(f"{location_name} {rc_no}번 경주 수집 중 오류: {e}")
                print(f"❌ {location_name} {rc_no}번 경주: 오류 발생")
                break
    
    print(f"\n=== 데이터 수집 완료 ===")
