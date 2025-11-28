# KorHorseRacing

다른 url도 정리해서 요청하기
1. 경주성적
 - url0="http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet=1&realRcDate=20170212&realRcNo=1"
2. 출전현황
 - "http://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do?Act=02&Sub=1&meet=1&rcNo=" + str(
no) + "&rcDate=" + date
3. 진료현황
 - "http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?"
        f"Act=02&Sub=1&meet={location}&rcNo={rc_no}&rcDate={date}"

db 사용은 아래 코드를 이용해줘 
- from autils.db.mysql_utils import MySQL
- ms = MySQL()
- ms.execute("SQL문") 으로 실행하면 돼.
- mysql_utils 파일 내용은 봐도 되는데, 수정은 하지마

step02파일은 내가 이미 수정했으니까, 놔두고 step03 파일 db사용 부분을 수정해 줘

4. 체중
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoWeight.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
5. 전적
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoRecord.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
6. 해당거리전적
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoDistanceRecord.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
7. 상대전적
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoMatchRecordList.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
8. 조교현황
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoTrainState.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
9. 말/기수 상관전적
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoRelationRecord.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
10. 최근 10회 전적
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfo10Score.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
11. 심판리포트
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoStewardsReport.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date
12. 출발조교현황
 - https://race.kra.co.kr/chulmainfo/chulmaDetailInfoStartingTrain.do?meet=1&rcNo=" + str(no) + "&rcDate=" + date



- 수집 url 참고

    - #폐출혈
    #url1="http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?Act=02&Sub=1&meet=1&rcNo=3&rcDate="+date

    - #원래URL
    #http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=1%B5%EE%B1%DE&csdkfjsf9ZVx11ja8a=skd8ahd8sh1sd1s

    - #1등급
    #http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=1%B5%EE%B1%DE

    - #국산마3등급
    #http://race.kra.co.kr/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=%B1%B93

    #4등급
    #%B1%B94

    #미검
    #/racehorse/profileList.do?Sub=1&meet=1&Act=07&rank=%BF%DC%B9%CC%B0%CB&csdkfjsf9ZVx11ja8a=skd8ahd8sh1sd1s
---


---
step01_collect_race_result.py 파일 구조를 참고해서, step02_get_entry.py 파일을 완성해 줘.
파일은 아래 작업을 수행할 수 있어야 해.
- url 데이터수집
- db table에 적재가 수행되야 해

db table 코드는 step01을 참고해
url은 아래와 같은 형식으로 사용해 
url = "http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?Act=02&Sub=1&meet=1&rcNo=" + str(
    no) + "&rcDate=" + date

잘 이해가 가지 않는 부분은 다시 물어봐도 돼


