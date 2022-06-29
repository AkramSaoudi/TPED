import glob
import os
import pandas as pd
import numpy as np
import pymysql
import string

from datetime import datetime


def create_table(co_cursor, table_name, table_schema):
    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    sql = "DROP TABLE IF EXISTS " + table_name
    co_cursor.execute(sql)
    sql = "CREATE TABLE " + table_name + "(" + table_schema + ")"
    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    co_cursor.execute(sql)

def get_date_id(date_str):
    date_str=date_str.replace('-', '')
    return date_str

def extracte_csv_to_DF(rootdir):
    data = []
    for file in os.listdir(rootdir):  # parcourire les 3 fichier algeria morocco tunisia
        d = os.path.join(rootdir, file)
        if os.path.isdir(d):  # parcourire les 3 csv de chaque fichier
            filenames = glob.glob(d + "/*.csv")
            for filename in filenames:
                df = pd.read_csv(filename, low_memory=False)
                data.append(df)
    df = pd.DataFrame(pd.concat(data))
    filter_col = [col for col in df if col.endswith('ATTRIBUTES')]
    df = df.drop(filter_col, axis=1)
    df = df.replace({np.nan: None})
    NAME_sattion = df['NAME'].str.split(", ", expand=True)
    df[['NAME', 'COUNTRY']] = NAME_sattion
    df['NAME'].astype('string')
    df = df.drop(df[df.COUNTRY == 'SP'].index)
    df["COUNTRY"].replace({"AG": "DZA", "TS": "TUN", "MO": "MAR"}, inplace=True)
    df = df.drop(['ACSH'], axis=1)

    return df

def populate_tabel_weather(cursor,df):

    for i, line in df.iterrows():
        sql = "INSERT INTO Weather_tabel(STATION, NAME, COUNTRY, LATITUDE, LONGITUDE, ELEVATION, DATE, PRCP, TAVG, TMAX, TMIN, " \
              "SNWD, PGTM, SNOW, WDFG,WSFG, WT01, WT03, WT05, WT07, WT08, WT09, WT16, WDFM, WSFM) VALUES(" \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s," \
              " %s)"

        cursor.execute(sql, (line.STATION,
                             line.NAME,
                             line.COUNTRY,
                             line.LATITUDE,
                             line.LONGITUDE,
                             line.ELEVATION,
                             line.DATE,
                             line.PRCP,
                             line.TAVG,
                             line.TMAX,
                             line.TMIN,
                             line.SNWD,
                             line.PGTM,
                             line.SNOW,
                             line.WDFG,
                             line.WSFG,
                             line.WT01,
                             line.WT03,
                             line.WT05,
                             line.WT07,
                             line.WT08,
                             line.WT09,
                             line.WT16,
                             line.WDFM,
                             line.WSFM))


shema_date="Date_ID VARCHAR(255)  PRIMARY KEY ," \
           "Date VARCHAR(255), " \
           "Day_Name VARCHAR(255)," \
           "Day_Name_Abbrev VARCHAR(255)," \
           "Day_Of_Month VARCHAR(255) ," \
           "Day_Of_Week VARCHAR(255)," \
           "Day_Of_Year VARCHAR(255)," \
           "Holiday_Name VARCHAR(255)," \
           "Is_Holiday VARCHAR(255)," \
           "Is_Weekday VARCHAR(255)," \
           "Is_Weekend VARCHAR(255)," \
           "Month_Abbrev VARCHAR(255)," \
           "Month_End_Flag VARCHAR(255)," \
           "Month_Name VARCHAR(255)," \
           "Month_Number VARCHAR(255)," \
           "Quarter VARCHAR(255)," \
           "Quarter_Name VARCHAR(255)," \
           "Quarter_Short_Name VARCHAR(255)," \
           "Same_Day_Previous_Year VARCHAR(255)," \
           "Same_Day_Previous_Year_ID VARCHAR(255)," \
           "Season VARCHAR(255)," \
           "Week_Begin_Date VARCHAR(255)," \
           "Week_Begin_Date_ID VARCHAR(255)," \
           "Week_Num_In_Month VARCHAR(255)," \
           "Week_Num_In_Year VARCHAR(255)," \
           "Year VARCHAR(255)," \
           "Year_And_Month VARCHAR(255)," \
           "Year_And_Month_Abbrev VARCHAR(255)," \
           "Year_And_Quarter VARCHAR(255) "
shema_lieu="STATION  VARCHAR(255) PRIMARY KEY ," \
           "NAME  VARCHAR(255) ," \
           "COUNTRY VARCHAR(255) ," \
           "LATITUDE  FLOAT , " \
           "LONGITUDE FLOAT," \
           "ELEVATION  FLOAT    "
shema_weather_fact=  "STATION VARCHAR(255)," \
                "Date_ID VARCHAR(255)," \
                "PRCP VARCHAR(255)," \
                "TAVG VARCHAR(255)," \
                "TMAX VARCHAR(255)," \
                "TMIN VARCHAR(255)," \
                "SNWD VARCHAR(255)," \
                "PGTM VARCHAR(255)," \
                "SNOW VARCHAR(255)," \
                "WDFG VARCHAR(255)," \
                "WSFG VARCHAR(255)," \
                "WT01 VARCHAR(255)," \
                "WT03 VARCHAR(255)," \
                "WT05 VARCHAR(255)," \
                "WT07 VARCHAR(255)," \
                "WT08 VARCHAR(255)," \
                "WT09 VARCHAR(255)," \
                "WT16 VARCHAR(255)," \
                "WDFM VARCHAR(255)," \
                "WSFM VARCHAR(255)," \
                "WT02 VARCHAR(255)," \
                "WT18 VARCHAR(255)," \
                "FOREIGN KEY (STATION) REFERENCES Lieu_Dim(STATION),"\
                "FOREIGN KEY (Date_ID) REFERENCES Date_Dim(Date_ID)," \
                "PRIMARY KEY (STATION, Date_ID)"
shema_Weather_tabel="id int NOT NULL AUTO_INCREMENT PRIMARY KEY, STATION VARCHAR(255) , NAME VARCHAR(255), COUNTRY VARCHAR(255), LATITUDE VARCHAR(255), LONGITUDE "\
             "VARCHAR(255), ELEVATION VARCHAR(255), DATE VARCHAR(255), PRCP VARCHAR(255), TAVG VARCHAR(255), TMAX "\
             "VARCHAR(255), TMIN VARCHAR(255), SNWD VARCHAR(255), PGTM VARCHAR(255), SNOW VARCHAR(255), WDFG VARCHAR(255), "\
             "WSFG VARCHAR(255), WT01 VARCHAR(255), WT03 VARCHAR(255), WT05 VARCHAR(255), WT07 VARCHAR(255), WT08 VARCHAR("\
             "255), WT09 VARCHAR(255), WT16 VARCHAR(255),  WDFM VARCHAR(255), WSFM VARCHAR(255), WT02 "\
             "VARCHAR(255), WT18 VARCHAR(255) "








def populate_dim_date(co_cursor):
        data = pd.read_csv("Dim_Date_1850-2050.csv")
        for index, line in data.iterrows():
           sql = "INSERT INTO date_dim (Date_ID,Date,Day_Name,Day_Name_Abbrev,Day_Of_Month,Day_Of_Week,Day_Of_Year," \
              "Holiday_Name,Is_Holiday,Is_Weekday,Is_Weekend,Month_Abbrev,Month_End_Flag,Month_Name,Month_Number," \
              "Quarter,Quarter_Name,Quarter_Short_Name,Same_Day_Previous_Year,Same_Day_Previous_Year_ID,Season," \
              "Week_Begin_Date,Week_Begin_Date_ID,Week_Num_In_Month,Week_Num_In_Year,Year,Year_And_Month," \
              "Year_And_Month_Abbrev,Year_And_Quarter)" \
              "VALUES (%s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                  "%s ," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s, " \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s," \
                 " %s) "
           holiday_name = line.Holiday_Name
           if pd.isna(line.Holiday_Name):
               holiday_name = None
           co_cursor.execute(sql, (line.Date_ID,
                                   line.Date,
                                   line.Day_Name,
                                   line.Day_Name_Abbrev,
                                   line.Day_Of_Month,
                                   line.Day_Of_Week,
                                   line.Day_Of_Year,
                                   holiday_name,
                                   line.Is_Holiday,
                                   line.Is_Weekday,
                                   line.Is_Weekend,
                                   line.Month_Abbrev,
                                   line.Month_End_Flag,
                                   line.Month_Name,
                                   line.Month_Number,
                                   line.Quarter,
                                   line.Quarter_Name,
                                   line.Quarter_Short_Name,
                                   line.Same_Day_Previous_Year,
                                   line.Same_Day_Previous_Year_ID,
                                   line.Season,
                                   line.Week_Begin_Date,
                                   line.Week_Begin_Date_ID,
                                   line.Week_Num_In_Month,
                                   line.Week_Num_In_Year,
                                   line.Year,
                                   line.Year_And_Month,
                                   line.Year_And_Month_Abbrev,
                                   line.Year_And_Quarter))

def populate_lieu_dim(co_cursor):
    co = pymysql.connect(host='localhost',
                         user='root',
                         password='1234',
                         database='weather',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    cursor_lieu =co.cursor()
    cursor_lieu.execute("SELECT DISTINCT  STATION, NAME,COUNTRY, LATITUDE, LONGITUDE, ELEVATION FROM weather_tabel")
    for line in cursor_lieu:
        query = "INSERT INTO lieu_Dim (STATION, NAME,COUNTRY, LATITUDE, LONGITUDE, ELEVATION) " \
                "VALUES (%s, %s, %s, %s, %s,%s)"
        co_cursor.execute(query, ( line["STATION"],line["NAME"],line["COUNTRY"], line["LATITUDE"], line["LONGITUDE"], line["ELEVATION"]))
#
#
#
#
#
#
#
#
#
def populate_weather_fact(co_cursor):
    co = pymysql.connect(host='localhost',
                                 user='root',
                                 password='1234',
                                 database='weather',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor_weather_fact = co.cursor()
    cursor_weather_fact.execute("SELECT STATION,"
                                " DATE,"
                                " PRCP,"
                                " TAVG,"
                                " TMAX,"
                                " TMIN,"
                                " SNWD,"
                                " PGTM,"
                                " SNOW,"
                                " WDFG,"
                                " WSFG,"
                                " WT01,"
                                " WT03,"
                                " WT05,"
                                " WT07,"
                                " WT08,"
                                " WT09,"
                                " WT16,"
                                " WDFM,"
                                " WSFM, "
                                " WT02,"
                                " WT18"
                                " FROM Weather_tabel")
    for line in cursor_weather_fact:
        query = "INSERT INTO `weatherdw`.`weather_fact` (STATION, Date_ID, PRCP, TAVG, TMAX, TMIN, SNWD, PGTM, SNOW, WDFG, WSFG, WT01, WT03, WT05, WT07, WT08, WT09, WT16, WDFM, WSFM, WT02, WT18) " \
                "VALUES " \
               "( %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s," \
                " %s )"
        co_cursor.execute(query, (line["STATION"],
                                 get_date_id(line["DATE"]),
                                 line["PRCP"],
                                 line["TAVG"],
                                 line["TMAX"],
                                 line["TMIN"],
                                 line["SNWD"],
                                 line["PGTM"],
                                 line["SNOW"],
                                 line["WDFG"],
                                 line["WSFG"],
                                 line["WT01"],
                                 line["WT03"],
                                 line["WT05"],
                                 line["WT07"],
                                 line["WT08"],
                                 line["WT09"],
                                 line["WT16"],
                                 line["WDFM"],
                                 line["WSFM"],
                                 line["WT02"],
                                 line["WT18"]))

connection_p = pymysql.connect(host='localhost',
                             user='root',
                             password='1234',
                             database='Weather',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor_p = connection_p.cursor()


connection = pymysql.connect(host='localhost',
                             user='root',
                             password='1234',
                             database='Weatherdw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()
create_table(cursor_p, "Weather_tabel",shema_Weather_tabel)
print("La table Weather_tabel est cree avec succes ")
#Create Data Warehouse schema
create_table(cursor,"Date_Dim",shema_date)
print("La table Date_Dim est cree avec succes ")
create_table(cursor,"weather_fact",shema_weather_fact)
print("La table weather_fact est cree avec succes ")
create_table(cursor,"lieu_Dim",shema_lieu)
print("La table lieu_Dim est cree avec succes ")

df=extracte_csv_to_DF('Weather Data')
populate_tabel_weather(cursor_p,df)
cursor_p.close()
connection_p.commit()
connection_p.close()
#
populate_dim_date(cursor)
populate_lieu_dim(cursor)
populate_weather_fact(cursor)





cursor.close()
connection.commit()
connection.close()






