from autils.db.mysql_utils import MySQL

ms = MySQL()

# ms.execute("DROP TABLE IF EXISTS hr_result")
ms.execute("""
    CREATE TABLE hr_result  
    (
        BAS_DT VARCHAR(10),
        meet VARCHAR(5),
        rc_no VARCHAR(5),
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
        primary key (BAS_DT, meet, rc_no, 마번)
    )
    """)

ms.execute("""CREATE TABLE IF NOT EXISTS hr_entry
           (`번호` VARCHAR(255),
            `마명` VARCHAR(255),
            `산지` VARCHAR(255),
            `성별` VARCHAR(255),
            `연령` VARCHAR(255),
            `레이팅` VARCHAR(255),
            `중량` VARCHAR(255),
            `증감` VARCHAR(255),
            `기수명` VARCHAR(255),
            `조교사명` VARCHAR(255),
            `마주명` VARCHAR(255),
            `조교횟수` VARCHAR(255),
            `출전주기` VARCHAR(255),
            `장구현황` VARCHAR(255),
            `특이사항` VARCHAR(255),
            `BAS_DT` VARCHAR(255),
            `rc_no` VARCHAR(255),
            `meet` VARCHAR(255),
            PRIMARY KEY (`BAS_DT`, `rc_no`, `meet`, `마번`)
           )""")

