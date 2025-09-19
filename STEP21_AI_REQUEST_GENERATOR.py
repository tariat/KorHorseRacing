# coding=utf-8
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from STEP01_COLLECT_RACE_RESULT import collect_race
import pandas as pd
from datetime import datetime
import logging

def generate_ai_request_text(location, date, rc_no):
    """
    경마 예측 분석을 위한 AI 요청 텍스트를 생성합니다.
    
    :param location: 경마장 (1: 서울, 3: 부산)
    :param date: 경주 날짜 (YYYYMMDD 형식)
    :param rc_no: 경주 번호
    :return: AI 요청 텍스트
    """
    try:
        # 경주 데이터 수집
        race_data = collect_race(location, date, rc_no, 1)
        
        if len(race_data) == 0:
            return "데이터를 가져올 수 없습니다. 날짜와 경기번호를 확인해주세요."
        
        # 경주 정보 추출
        race_name = race_data['day_th'].iloc[0] if 'day_th' in race_data.columns else f"제{rc_no}경주"
        distance = race_data['distance'].iloc[0] if 'distance' in race_data.columns else "정보없음"
        weather = race_data['weather'].iloc[0] if 'weather' in race_data.columns else "정보없음"
        track_condition = race_data['race_st'].iloc[0] if 'race_st' in race_data.columns else "정보없음"
        location_name = "서울" if location == 1 else "부산"
        
        # AI 요청 텍스트 생성
        ai_request = f"""경마 예측 분석을 요청합니다.

**경주 정보:**
- 경주명: {race_name} ({location_name})
- 거리: {distance}
- 트랙 상태: {track_condition}
- 날씨: {weather}

**출전마 정보:**
"""
        
        # 각 말의 정보 추가
        for idx, row in race_data.iterrows():
            horse_num = idx + 1
            horse_name = row.get('마명', '정보없음')
            jockey = row.get('기수명', '정보없음')
            age_gender = f"{row.get('연령', '정보없음')}/{row.get('성별', '정보없음')}"
            weight = row.get('중량', '정보없음')
            rating = row.get('레이팅', '정보없음')
            
            # 최근 성적 정보 (실제로는 더 상세한 데이터가 필요하지만 현재 가능한 정보로 대체)
            recent_performance = "데이터 수집 필요"
            distance_performance = "데이터 수집 필요"
            
            ai_request += f"""{horse_num}. 말 이름: {horse_name}
   - 기수: {jockey}
   - 최근 5경주 성적: {recent_performance}
   - 이 거리에서의 성적: {distance_performance}
   - 나이/성별: {age_gender}
   - 부담중량: {weight}kg
   - 레이팅: {rating}

"""
        
        ai_request += """**분석 요청:**
각 말의 승률을 객관적 데이터에 기반해 분석하고, 상위 3위까지 예상 순위와 그 근거를 제시해주세요."""
        
        return ai_request
        
    except Exception as e:
        logging.error(f"AI 요청 텍스트 생성 중 오류: {e}")
        return f"오류가 발생했습니다: {e}"

def save_ai_request_to_file(location, date, rc_no, output_dir="ai_requests"):
    """
    AI 요청 텍스트를 파일로 저장합니다.
    
    :param location: 경마장 (1: 서울, 3: 부산)
    :param date: 경주 날짜 (YYYYMMDD 형식)
    :param rc_no: 경주 번호
    :param output_dir: 출력 디렉토리
    :return: 저장된 파일 경로
    """
    # 출력 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # AI 요청 텍스트 생성
    ai_request_text = generate_ai_request_text(location, date, rc_no)
    
    # 파일명 생성
    location_name = "서울" if location == 1 else "부산"
    filename = f"AI_분석요청_{location_name}_{date}_{rc_no}경주.txt"
    filepath = os.path.join(output_dir, filename)
    
    # 파일 저장
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(ai_request_text)
        
        print(f"✅ AI 분석 요청 파일이 저장되었습니다: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"❌ 파일 저장 중 오류 발생: {e}")
        return None

def main():
    """
    메인 함수 - 사용자 입력을 받아 AI 요청 텍스트를 생성하고 저장합니다.
    """
    logging.basicConfig(level=logging.ERROR)
    
    print("=== 경마 AI 분석 요청 텍스트 생성기 ===")
    print()
    
    try:
        # 사용자 입력 받기
        print("경마장을 선택하세요:")
        print("1. 서울")
        print("3. 부산")
        location = int(input("경마장 번호를 입력하세요 (1 또는 3): "))
        
        if location not in [1, 3]:
            print("잘못된 경마장 번호입니다. 1(서울) 또는 3(부산)을 입력해주세요.")
            return
        
        date = input("경주 날짜를 입력하세요 (YYYYMMDD 형식, 예: 20241201): ")
        
        # 날짜 형식 검증
        try:
            datetime.strptime(date, '%Y%m%d')
        except ValueError:
            print("날짜 형식이 올바르지 않습니다. YYYYMMDD 형식으로 입력해주세요.")
            return
        
        rc_no = int(input("경주 번호를 입력하세요 (1-12): "))
        
        if not 1 <= rc_no <= 12:
            print("경주 번호는 1-12 사이의 숫자여야 합니다.")
            return
        
        print(f"\n데이터를 수집하고 AI 요청 텍스트를 생성하고 있습니다...")
        
        # AI 요청 텍스트 생성 및 저장
        saved_file = save_ai_request_to_file(location, date, rc_no)
        
        if saved_file:
            print(f"\n🎯 완료! 다음 파일에서 생성된 텍스트를 확인할 수 있습니다:")
            print(f"   {saved_file}")
            
            # 생성된 텍스트 미리보기
            with open(saved_file, 'r', encoding='utf-8') as f:
                preview = f.read()[:500]
            
            print(f"\n📋 생성된 텍스트 미리보기:")
            print("=" * 50)
            print(preview + "...")
            print("=" * 50)
        
    except KeyboardInterrupt:
        print("\n프로그램이 중단되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")

def quick_generate(location, date, rc_no):
    """
    빠른 생성 함수 - 매개변수로 바로 호출 가능
    
    사용 예시:
    quick_generate(1, "20241201", 1)  # 서울 2024년 12월 1일 1경주
    """
    saved_file = save_ai_request_to_file(location, date, rc_no)
    return saved_file

if __name__ == "__main__":
    main()