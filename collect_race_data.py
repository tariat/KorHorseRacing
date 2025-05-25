# coding=utf-8

from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

import pandas as pd
import numpy as np
import sqlite3

from datetime import datetime, timedelta
import load_horse_no


def collect_data_by_url(url):
    """
        url에서 data 수집
        # url = "http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet=1&realRcDate=20170723&realRcNo=1"

        - 기수현황은 없어진 듯
    """
    tables = pd.read_html(url, encoding='cp949')

    # 경주별 상세 성적표
    df = tables[3].copy()
    col_lst = [c[1].replace(" ","") for c in df.columns]
    df.columns = col_lst

    # 경주 정보
    df['경기등급'] = tables[0].iloc[1][0] #경기등급
    df['distance'] = tables[0].iloc[1][1] #distance
    df['경기종류1'] = tables[0].iloc[1][2] #경기종류1    
    df['경기종류2'] = tables[0].iloc[1][3] #경기종류2
    df['경기종류3'] = tables[0].iloc[1][4] #경기종류3
    df['경기종류4'] = tables[0].iloc[1][6] #경기종류4
    df['weather'] = tables[0].iloc[0][5] #날씨
    df['road_cd'] = tables[0].iloc[0][6] #지면상태
    df['경기시간'] = tables[0].iloc[0][8] #경기시간
    df = df.rename(columns={'마번': '번호', '경주기록': 'race_time'})

    #자료 변환하기 - 속도
    df['distance'] = df['distance'].str[:-1].astype(int)
    time_parts = df['race_time'].str.split(':', expand=True)
    
    # 숫자 변환 시 에러가 있는 경우 NaN으로 처리
    minutes = pd.to_numeric(time_parts[0], errors='coerce')
    seconds = pd.to_numeric(time_parts[1], errors='coerce')
    
    # 초 단위로 변환
    df['y'] = minutes * 60 + seconds
    df['speed'] = df['distance'] / df['y']

    # 베팅정보 추출
    bokseung_text = str(tables[6].iloc[1, 1])
    bokseung_match = re.search(r'복승식:\s*[①-⑳]+\s*([\d.]+)', bokseung_text)
    bokseung_value = float(bokseung_match.group(1)) if bokseung_match else None

    # 삼복승 값 추출 (2행 2열에서)
    sambokseung_text = str(tables[6].iloc[2, 2])
    sambokseung_match = re.search(r'삼복승식:\s*[①-⑳]+\s*([\d.]+)', sambokseung_text)
    sambokseung_value = float(sambokseung_match.group(1)) if sambokseung_match else None

    df["복승식"] = float(bokseung_value)
    df["삼복승식"] = float(sambokseung_value)

    return df


def day_update(date, location="bu"):

    # url = "http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet=1&realRcDate=20170723&realRcNo=1"

    con = sqlite3.connect("./data/race_db.db")

    total_last = pd.DataFrame()

    for no in range(1, 20):
        try:
            url0 = "http://race.kra.co.kr/raceScore/ScoretableDetailList.do?meet=1&realRcDate=" + date + "&realRcNo=" + str(
                no)

            #d0 = der_1(url0) #This function is not defined
            d1, rider = collect_data_by_url(url0, location)

            #d0["date"] = date #d0 is not defined
            d1["date"] = date
            d1["no"] = no

            rider["date"] = date
            rider["no"] = no

            url1 = "http://race.kra.co.kr/chulmainfo/chulmaDetailInfoAccessoryState.do?Act=02&Sub=1&meet=1&rcNo=" + str(
                no) + "&rcDate=" + date
            #d2 = blood(url1) #This function is not defined

            #total = pd.merge(d0, d1, on=u"번호") #d0 is not defined
            #total = pd.merge(total, d2, on=u"번호") #d2 is not defined
            total = d1

            total = total.rename(columns={u'마명': 'horse_name', 'S1F-1C-2C-3C-4C-G1F': 'path'})

            total["연령"] = total["연령"].str[0:-1].apply(pd.to_numeric)

            total["path"] = total["path"].str.replace("\n", "")
            total["path"] = total["path"].str.replace("\r", "")
            total["path"] = total["path"].str.replace(" ", "")

            total["apply_end"] = 0
            total.ix[total['horse_name'].str[0] == "★", "apply_end"] = 1
            total.ix[total['horse_name'].str[0] == "★", "horse_name"] = total['horse_name'].str.slice(
                1)

            total["is_horse_bu"] = 0
            total.ix[total['horse_name'].str[0:3] == "[부]", "is_horse_bu"] = 1
            total.ix[total['horse_name'].str[0:3] == "[부]", "horse_name"] = total[
                'horse_name'].str.slice(3)

            total['weight_up'] = 0
            total.ix[total['중량'].str.get(0) == "*", ['weight_up']] = 1
            total.ix[total['중량'].str.get(0) == "*", ['중량']] = total['중량'].str.slice(1)

            total["wins"] = "0"
            total.ix[total["기수명"].str.get(0) == "(", "wins"] = total["기수명"].str.split("(").str.get(
                1).astype(str).str.split(")").str.get(0)
            total.ix[total["기수명"].str.get(0) == "(", "기수명"] = total["기수명"].str.split(")").str.get(1)

            total["horse_weight"] = total["마체중"].str.split("(").str.get(0)
            total["horse_weight_pm"] = total["마체중"].str.split(")").str.get(0).str.split("(").str.get(1)
            total = total.drop({"마체중"}, axis=1)

            total_last = pd.concat([total, total_last])

            print("Success: " + date + "," + str(no))

        except Exception as e:
            print("Failed: " + date + "," + str(no), end=", ")
            print(e)
            break

    # new_total.to_csv(filename,encoding="utf-8")
    con.close()

    return total_last, rider

"""
df1=load_horse_no.der()
df1=df1.reset_index(drop=True)

#20161112
#20161113

total, rider=day_update("20161113")
total1=pd.merge(total,df1,how='left',on=u"horse_name")

con = sqlite3.connect("./data/race_db.db")
total1.to_sql('total_hn', con, if_exists='append',index=False)
rider.to_sql('rider', con, if_exists='append',index=False)
con.close()
"""

def update():
    con = sqlite3.connect("./data/race_db_bu.db")
    cur = con.cursor()
    query = cur.execute("SELECT date From total_hn_bu")
    cols = [column[0] for column in query.description]
    total = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    max_date = total["date"].max()
    max_date = str(max_date)

    print("DB에 저장된 최근 날짜는 " + max_date + "입니다.")

    max_date1 = datetime.date(int(max_date[0:4]), int(max_date[4:6]), int(max_date[6:8]))
    max_date1 = max_date1 + datetime.timedelta(days=1)

    today = datetime.datetime.today().strftime("%Y%m%d")
    today1 = datetime.date(int(today[0:4]), int(today[4:6]), int(today[6:8]))
    today1 = today1 + datetime.timedelta(days=1)

    update = pd.DataFrame()
    update_rider = pd.DataFrame()

    while max_date1 < today1:

        date = str(max_date1)
        date = date.replace("-", "")

        for no in range(1, 20):
            try:
                total, rider = day_update(date, "bu")
                update = pd.concat([update, total])
                update_rider = pd.concat([update, rider])

                print("Busan, Success: " + date + "," + str(no))
            except Exception as e:
                print("Busan, Failed: " + date + "," + str(no), end=", ")
                print(e)
                break

        for no in range(1, 20):
            try:
                total, rider = day_update(date, "seo")
                update = pd.concat([update, total])

                print("Seoul, Success: " + date + "," + str(no))
            except Exception as e:
                print("Seoul, Failed: " + date + "," + str(no), end=", ")
                print(e)
                break

        max_date1 = max_date1 + datetime.timedelta(days=1)

        #if (len(update) == 0): #This condition is not needed
        #    print("업데이트할 내용이 없습니다.")
        #    return 0

    df1 = load_horse_no.der()
    df1 = df1.reset_index(drop=True)
    update1 = pd.merge(update, df1, how='left', on=u"horse_name")

    update1["horse_no"] = update1["horse_no"].astype(int)

    #####################################################################
    # rider는 부산 것만 일단 업데이트 한다. 전에 것을 보았더니 서울은 업데이트를 안 했더라 #
    #####################################################################
    update1.to_sql('total_hn_bu', con, if_exists='append', index=False)
    #update_rider.to_sql('rider', con, if_exists='append', index=False) #rider is not defined
    con.close()

    return 0

"""
date="20160910"
no=1
"""
def verify_data():

    con = sqlite3.connect("./data/race_db_bu.db")
    cur = con.cursor()
    query = cur.execute("SELECT * From total_hn_bu")
    cols = [column[0] for column in query.description]
    race_db = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    race_day = race_db[["date", "no"]]
    race_day = race_day.drop_duplicates(keep="first")
    race_day["date"] = race_day["date"].astype(str)

    ym = list(["201201", "201202", "201203", "201204", "201205", "201206", "201207", "201208", "201209", "201210",
               "201211", "201212",
               "201301", "201302", "201303", "201304", "201305", "201306", "201307", "201308", "201309", "201310",
               "201311", "201312",
               "201401", "201402", "201403", "201404", "201405", "201406", "201407", "201408", "201409", "201410",
               "201411", "201412",
               "201501", "201502", "201503", "201504", "201505", "201506", "201507", "201508", "201509", "201510",
               "201511", "201512", "201601", "201602", "201603", "201604", "201605", "201606", "201607", "201608",
               "201609", "201610", "201611", "201612",
               "201701", "201702", "201703", "201704", "201705"])

    temp = pd.DataFrame()

    for a in range(len(ym)):
        for day in range(1, 32):
            try:
                if day < 10:
                    date = ym[a] + "0" + str(day)
                else:
                    date = ym[a] + str(day)

                for no in range(1, 20):
                    try:
                        if len(race_day[(race_day["date"] == date) & (race_day["no"] == no)]) > 0:
                            print(date + "-" + str(no) + " isin.")
                            pass
                        else:
                            total, rider = day_update(date, "bu")

                            total.to_sql('total_bu', con, if_exists='append', index=False)
                            rider.to_sql('rider_bu', con, if_exists='append', index=False)

                            temp = pd.concat([total, temp])

                            print("Success: " + date + "," + str(no))
                    except Exception as e:
                        print("Failed: " + date + "," + str(no), end=", ")
                        print(e)
                        break

            except Exception as e:
                print("Day  is out: " + date + "," + str(no), end=", ")
                print(e)
                break

    df1 = load_horse_no.der()
    df1 = df1.reset_index(drop=True)
    temp1 = pd.merge(temp, df1, how='left', on=u"horse_name")

    print("말번호가 없는 거:", end=" ")
    print(temp1[temp1["horse_no"].isnull()])

    temp1.to_sql('total_hn_bu', con, if_exists='append', index=False)
    con.close()

    return 0

def elo_update():
    con = sqlite3.connect("./data/race_db_bu.db")
    cur = con.cursor()

    query = cur.execute("SELECT * From total_hn_bu")
    cols = [column[0] for column in query.description]
    race = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    query = cur.execute("SELECT * From horse_elo_rating")
    cols = [column[0] for column in query.description]
    horse_list = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    race.groupby("순위").size()
    race=race[race["순위"]!=""]
    race["순위"]=race["순위"].astype(float)
    race["date"]=race["date"].astype(int)

    date_list=race[["date","no","location"]]
    date_list=date_list.drop_duplicates(keep="first")
    date_list=date_list.sort_values(["date","no","location"])
    date_list=date_list.reset_index(drop=True)

    hdate_list=horse_list[["date"]]
    hdate_list["date"]=hdate_list["date"].astype(float)
    hdate_list=hdate_list.drop_duplicates(keep="first")
    hdate_list=hdate_list.sort_values("date")
    hdate_list=hdate_list.reset_index(drop=True)

    date_list=date_list[~date_list["date"].isin(hdate_list["date"])]
    date_list=date_list.reset_index(drop=True)

    if len(date_list)==0:
        return 0

    k = 6000

    # date_for=52
    for date_for in range(len(date_list)):
        temp=race[(race["date"]==date_list["date"][date_for]) & (race["no"]==date_list["no"][date_for])  & (race["location"]==date_list["location"][date_for])]
        temp=temp.reset_index(drop=True)
        len_horses=len(temp)

        #기존에 없는 말의 경우, 초기값을  설정한다.
        for x in range(len_horses):
            if len(horse_list[horse_list["horse_no"]==temp["horse_no"][x]])==0:
                horse_list=pd.concat([horse_list,pd.DataFrame([[temp["date"][x]-1, temp["horse_no"][x], 1500]], columns=["date", "horse_no", "horse_rating"])])

        new_horse_list=pd.DataFrame(columns=["date","horse_no","horse_rating"])
        # x=0
        for x in range(len_horses):
            # 가장 최근의 R을 가져온다.
            horse=horse_list[horse_list["horse_no"]==temp["horse_no"][x]]
            horse=horse.sort_values("date",ascending=False)
            horse=horse.drop_duplicates("horse_no",keep="first")

            s=(len_horses - temp["순위"][x]) / (len_horses*(len_horses-1)/2)

            e=0
            for y in range(len_horses):
                if x==y:
                    pass
                else:
                    ri = horse_list[horse_list["horse_no"] == temp["horse_no"][y]]
                    ri = ri.sort_values(by="date",ascending=False)
                    ri = ri.reset_index(drop=True)
                    ri = ri.loc[0,"horse_rating"]

                    rx = horse_list[horse_list["horse_no"] == temp["horse_no"][x]]
                    rx = rx.sort_values(by="date",ascending=False)
                    rx = rx.reset_index(drop=True)
                    rx = rx.loc[0, "horse_rating"]

                    pre_e=1/(1+10**(ri-rx))
                    e=e+pre_e

            e= e / (len_horses * (len_horses - 1) / 2)
            new_r = rx + k * (s-e)
            new_horse_list=pd.concat([new_horse_list,pd.DataFrame([[temp["date"][x], temp["horse_no"][x], new_r]], columns=["date", "horse_no", "horse_rating"])])
            # print(new_horse_list)

        horse_list = pd.concat([horse_list, new_horse_list])
        print(str(date_for) + " is completed!")

    horse_list.to_sql("horse_elo_rating", con, if_exists='replace', index=False)


    ###  기수 관련 사항
    query = cur.execute("SELECT * From rider_elo_rating")
    cols = [column[0] for column in query.description]
    rider_list = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    rider_list["date"]=rider_list["date"].astype(float)

    k = 50

    # date_for=52
    for date_for in range(len(date_list)):
        temp=race[(race["date"]==date_list["date"][date_for]) & (race["no"]==date_list["no"][date_for]) & (race["location"]==date_list["location"][date_for])]
        temp=temp.reset_index(drop=True)
        len_rider=len(temp)

        #기존에 없는 말의 경우, 초기값을  설정한다.
        for x in range(len_rider):
            if len(rider_list[rider_list["기수명"]==temp["기수명"][x]])==0:
                rider_list=pd.concat([rider_list,pd.DataFrame([[temp["date"][x]-1, temp["기수명"][x], 1500]], columns=["date", "기수명", "rider_rating"])])

        new_rider_list=pd.DataFrame(columns=["date","기수명","rider_rating"])
        # x=0
        for x in range(len_rider):
            # 가장 최근의 R을 가져온다.
            horse=rider_list[rider_list["기수명"]==temp["기수명"][x]]
            horse=horse.sort_values("date",ascending=False)
            horse=horse.drop_duplicates("기수명",keep="first")

            s=(len_rider - temp["순위"][x]) / (len_rider*(len_rider-1)/2)

            e=0
            for y in range(len_rider):
                if x==y:
                    pass
                else:
                    ri = rider_list[rider_list["기수명"] == temp["기수명"][y]]
                    ri = ri.sort_values(by="date",ascending=False)
                    ri = ri.reset_index(drop=True)
                    ri = ri.loc[0,"rider_rating"]

                    rx = rider_list[rider_list["기수명"] == temp["기수명"][x]]
                    rx = rx.sort_values(by="date",ascending=False)
                    rx = rx.reset_index(drop=True)
                    rx = rx.loc[0, "rider_rating"]

                    pre_e=1/(1+10**(ri-rx))
                    e=e+pre_e

            e= e / (len_rider * (len_rider - 1) / 2)
            new_r = rx + k * (s-e)
            new_rider_list=pd.concat([new_rider_list,pd.DataFrame([[temp["date"][x], temp["기수명"][x], new_r]], columns=["date", "기수명", "rider_rating"])])
            # print(new_rider_list)

        rider_list=pd.concat([rider_list,new_rider_list])
        # print(rider_list)
        print(str(date_for)+" is completed!")

    rider_list.to_sql("rider_elo_rating", con, if_exists='replace', index=False)


    ###  조교사 관련 사항

    query = cur.execute("SELECT * From trainer_elo_rating")
    cols = [column[0] for column in query.description]
    trainer_list = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    trainer_list["date"] = trainer_list["date"].astype(float)

    k = 50

    # date_for=52
    for date_for in range(len(date_list)):
        temp = race[(race["date"] == date_list["date"][date_for]) & (race["no"] == date_list["no"][date_for]) & (race["location"] == date_list["location"][date_for])]
        temp = temp.reset_index(drop=True)
        len_trainer = len(temp)

        # 기존에 없는 말의 경우, 초기값을  설정한다.
        for x in range(len_trainer):
            if len(trainer_list[trainer_list["조교사명"] == temp["조교사명"][x]]) == 0:
                trainer_list = pd.concat([trainer_list, pd.DataFrame([[temp["date"][x] - 1, temp["조교사명"][x], 1500]],
                                                                 columns=["date", "조교사명", "trainer_rating"])])

        new_trainer_list = pd.DataFrame(columns=["date", "조교사명", "trainer_rating"])
        # x=0
        for x in range(len_trainer):
            # 가장 최근의 R을 가져온다.
            horse = trainer_list[trainer_list["조교사명"] == temp["조교사명"][x]]
            horse = horse.sort_values("date", ascending=False)
            horse = horse.drop_duplicates("조교사명", keep="first")

            s = (len_trainer - temp["순위"][x]) / (len_trainer * (len_trainer - 1) / 2)

            e = 0
            for y in range(len_trainer):
                if x == y:
                    pass
                else:
                    ri = trainer_list[trainer_list["조교사명"] == temp["조교사명"][y]]
                    ri = ri.sort_values(by="date", ascending=False)
                    ri = ri.reset_index(drop=True)
                    ri = ri.loc[0, "trainer_rating"]

                    rx = trainer_list[trainer_list["조교사명"] == temp["조교사명"][x]]
                    rx = rx.sort_values(by="date", ascending=False)
                    rx = rx.reset_index(drop=True)
                    rx = rx.loc[0, "trainer_rating"]

                    pre_e = 1 / (1 + 10 ** (ri - rx))
                    e = e + pre_e

            e = e / (len_trainer * (len_trainer - 1) / 2)
            new_r = rx + k * (s - e)
            new_trainer_list = pd.concat([new_trainer_list, pd.DataFrame([[temp["date"][x], temp["조교사명"][x], new_r]],
                                                                     columns=["date", "조교사명", "trainer_rating"])])
            # print(new_trainer_list)

        trainer_list = pd.concat([trainer_list, new_trainer_list])
        # print(trainer_list)
        print(str(date_for) + " is completed!")

    trainer_list.to_sql("trainer_elo_rating", con, if_exists='replace', index=False)

    con.close()

    return 1


if __name__ == "__main__":

    verify()

    """
    #day업데이트
    #day_update("20160820")


    #경주 기록 만들어어오기
    ym=list(["201201","201202","201203","201204","201205","201206","201207","201208","201209","201210","201211","201212",
             "201301","201302","201303","201304","201305","201306","201307","201308","201309","201310","201311","201312",
             "201401","201402","201403","201404","201405","201406","201407","201408","201409","201410","201411","201412",
             "201501", "201502", "201503", "201504", "201505", "201506", "201507", "201508", "201509", "201510",
             "201511", "201512","201601","201602","201603","201604","201605","201606","201607","201608","201609","201610","201611","201612",
             "201701","201702","201703","201704","201705"])


    con = sqlite3.connect("./data/race_db_bu.db")
    cursor = con.cursor()

    for a in range(len(ym)):
        for day in range(1,32):
            try:
                if day<10:
                    date=ym[a]+"0"+str(day)
                else:
                    date=ym[a]+str(day)

                for no in range(1,20):
                    try:

                        total, rider = make_day_bu(date,no)

                        total.to_sql('total_bu', con, if_exists='append',index=False)
                        rider.to_sql('rider_bu', con, if_exists='append',index=False)

                        print("Success: "+date+","+str(no))

                    except Exception as e:
                        print("Failed: " + date + "," + str(no), end=", ")
                        print(e)
                        break
            except Exception as e:
                print("Day Exception:"+date,end=", ")
                print(e)
                break

    query = cursor.execute("SELECT * From total_bu")
    cols = [column[0] for column in query.description]
    race_db = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    print("race_db:"+str(len(race_db)))

    df1=load_horse_no.der()
    df1=df1.reset_index(drop=True)
    total=pd.merge(race_db,df1,how='left',on=u"horse_name")

    total.to_sql('total_hn_bu', con, if_exists='replace',index=False)
    con.close()
    """
