import xlrd
from datetime import datetime, timedelta
import os
import pandas as pd
from tabulate import tabulate
import math
import numpy as np
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta


class DBhandler:
    def __init__(self, db_path=None, type_="sqlite3"):
        """
        """
        if not db_path:
            raise Exception("db_path is required")

        if type_ == "pyodbc":
            import pyodbc
            conection_config = "DRIVER={SQLite3 ODBC Driver};SERVER=localhost;DATABASE=%s;Trusted_connection=yes" % db_path
            self.connection = pyodbc.connect(conection_config)
            self.cursor = self.connection.cursor()
        elif type_ == "sqlite3":
            import sqlite3
            self.connection = sqlite3.connect(db_path)
            self.cursor = self.connection.cursor()
        else:
            raise Exception("Incorrect type")

    def execute_sql(self, raw_sql, commit=False):
        self.cursor.execute(raw_sql)
        if commit:
            self.connection.commit()

    def create_table(self, type_=None):
        if not type_:
            raise Exception("type_ is required")
        if type_ == "profile":
            sql_query = """CREATE TABLE IF NOT EXISTS profile
            (
                id INTEGER NOT NULL,
                birth TEXT,
                gender TEXT,
                employed_by TEXT,
                issue_date TEXT,
                education TEXT,
                children INTEGER,
                family INTEGER,
                marital_status TEXT,
                position TEXT,
                housing TEXT,
                income INTEGER,
                age_of_car INTEGER,
                house_ownership INTEGER,
                income_type TEXT
            )"""
        elif type_ == "contract":
            sql_query = """CREATE TABLE IF NOT EXISTS contract
            (
                id INTEGER NOT NULL,
                contract_id INTEGER,
                amount INTEGER,
                type TEXT,
                month_term INTEGER,
                annuity INTEGER,
                contract_date TEXT
            )"""
        elif type_ == "payment":
            sql_query = """CREATE TABLE IF NOT EXISTS payment
            (
                contract_id INTEGER NOT NULL,
                payment_date TEXT,
                amount_due REAL,
                amount_paid REAL
            )
            """
        elif type_ == "payment_delay":
            sql_query = """CREATE TABLE IF NOT EXISTS payment_delay
            (
                contract_id INTEGER NOT NULL,
                payment_date TEXT,
                payment_delay INTEGER,
                status TEXT
            )"""
        elif type_ == "payment_delay_max":
            sql_query = """CREATE TABLE IF NOT EXISTS payment_delay_max
            (
              contract_id INTEGER NOT NULL,
              payment_delay INTEGER
            )"""
        elif type_ == "temp":
            sql_query = """CREATE TABLE IF NOT EXISTS temp
            (
                payment_day1 TEXT,
                payment_delay INTEGER,
                status TEXT,
                contract_id INTEGER NOT NULL
            )"""
        else:
            sql_query = ""

        self.execute_sql(raw_sql=sql_query, commit=True)

    def join_tables(self, table1, table2, left_key, right_key):
        sql_template = """CREATE TABLE IF NOT EXISTS {}_{} AS
            SELECT * FROM {} as table1
            JOIN {} as table2
                ON table1.{} = table2.{}
        """.format(table1, table2, table1, table2, left_key, right_key)
        self.cursor.execute(sql_template)
        self.connection.commit()

    def create_map(self, values_map, target_table, target_column):
        create_sql = """CREATE TABLE IF NOT EXISTS values_map (key TEXT, value TEXT, table_name TEXT)"""
        self.cursor.execute(create_sql)
        self.connection.commit()

        insert_temp = """INSERT INTO values_map VALUES (?, ?, ?)"""
        update_temp = """UPDATE {} SET {} = '{}' WHERE {} = '{}'"""
        for pair in values_map.items():
            self.cursor.execute(insert_temp, [pair[0], pair[1], target_table])
            self.cursor.execute(update_temp.format(target_table, target_column, pair[1], target_column, pair[0]))
        self.connection.commit()


def check_date(date_str):
    try:
        datetime.strptime(date_str, "%m.%d.%Y")
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def create_profile(connection, data_path):
    connection.create_table(type_="profile")
    # open each workbook and parse the data
    profile_data = os.path.join(data_path, "profile")
    # create a insert query template
    insert_query = """INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    # create a main loop for file processing
    for file_name in os.listdir(profile_data):
        file = os.path.join(profile_data, file_name)
        data = xlrd.open_workbook(file)
        sheet = data.sheet_by_index(0)

        labels = ["Identity Number", "Date of Birth", "Gender", "Employed By", "Issue Date", "Education",
                  "Children", "Family", "Marital Status", "Position", "Housing", "Income", "Age of Car (if owned)",
                  "House ownership", "Income Type"]
        values = [None for _ in range(len(labels))]
        row = sheet.nrows
        column = sheet.ncols
        for i in range(row):
            for j in range(column):
                for k in range(len(labels)):
                    if sheet.cell(i, j).value == labels[k]:
                        if labels[k] in ("Date of Birth", "Issue Date"):
                            if check_date(sheet.cell(i + 1, j).value):
                                values[k] = sheet.cell(i + 1, j).value
                            else:
                                temp = datetime.fromordinal(int(sheet.cell(i + 1, j).value))
                                temp = datetime(day=temp.day, month=temp.month, year=temp.year + 1900)
                                temp = temp.strftime("%m.%d.%Y")
                                values[k] = temp
                        elif labels[k] == 'House ownership':
                            if sheet.cell(i + 1, j).value == 'Y':
                                values[k] = 1
                            elif sheet.cell(i + 1, j).value == 'N':
                                values[k] = 0
                            else:
                                values[k] = None
                        else:
                            try:
                                values[k] = sheet.cell(i + 1, j).value
                            except IndexError:
                                values[k] = None
        connection.cursor.execute(insert_query, values)
    connection.connection.commit()


def create_contract(connection, data_path):
    connection.create_table(type_="contract")
    # open each workbook and parse the data
    profile_data = os.path.join(data_path, "contracts")
    # create a insert query template
    insert_query = """INSERT INTO contract VALUES (?, ?, ?, ?, ?, ?, ?)"""
    for file_name in os.listdir(profile_data):
        file = os.path.join(profile_data, file_name)
        data = xlrd.open_workbook(file)
        sheet = data.sheet_by_index(0)
        labels = ['Identity Number', 'Contract Number', 'Amount', 'Type', 'Term (month)', 'Annuity']
        values = [None for _ in range(len(labels) + 1)]
        row = sheet.nrows
        column = sheet.ncols
        for i in range(row):
            for j in range(column):
                for k in range(len(labels)):
                    if sheet.cell(i, j).value == labels[k]:
                        values[k] = sheet.cell(i + 1, j).value

        if check_date(sheet.cell(1, 5).value):
            values[-1] = sheet.cell(1, 5).value
        else:
            temp = datetime.fromordinal(int(sheet.cell(1, 5).value))
            temp = datetime(day=temp.day, month=temp.month, year=temp.year + 1900)
            temp = temp.strftime("%m.%d.%Y")
            values[-1] = temp
        connection.cursor.execute(insert_query, values)
    connection.connection.commit()


def create_payments(connection, data_path):
    connection.create_table(type_="payment")
    insert_query = """INSERT INTO payment VALUES (?, ?, ?, ?)"""
    excel = xlrd.open_workbook(data_path)
    sheet = excel.sheet_by_index(0)
    data = [sheet.col_values(0)[1::], sheet.col_values(1)[1::], sheet.col_values(2)[1::], sheet.col_values(3)[1::]]
    for data in zip(sheet.col_values(0)[1::], sheet.col_values(1)[1::], sheet.col_values(2)[1::],
                    sheet.col_values(3)[1::]):
        values = [None, None, None, None]
        if check_date(data[1]):
            values[1] = data[1]
        else:
            temp = datetime.fromordinal(int(data[1]))
            temp = datetime(day=temp.day, month=temp.month, year=temp.year + 1900)
            temp = temp.strftime("%m.%d.%Y")
            values[1] = temp
        values[0] = int(data[0])
        values[2] = float(data[2])
        values[3] = float(data[3])
        connection.cursor.execute(insert_query, values)
    connection.connection.commit()


def check_input(string):
    if '"' in string:
        return string, "str"
    elif "'" in string:
        return string, "str"
    else:
        try:
            return int(string), "int"
        except ValueError:
            print("Values is not int")
            try:
                return float(string), "float"
            except ValueError:
                print("Value is not float")


def generate_range(strat_date, end_date, horizon):
    points = [strat_date]
    while points[-1] < end_date:
        new_date = points[-1] + timedelta(days=horizon)
        points.append(new_date)
    return points


def write_dataframe(dataframe, connection, sql_template):
    for _, row in dataframe.iterrows():
        connection.cursor.execute(sql_template, [i for i in row])
    connection.connection.commit()


def resample_frame(df, days, key):
    df = df.set_index(key)
    return df.resample('%sD' % str(days))


def search_engine(data):
    search = input("Enter parameter for filtering resulting values (Format 'column_name') _>")
    if search not in data.columns:
        print("Can't find column")
        return 0, 0, 0
    good_data = data[data["status"] == "Non-Default"].copy()
    bad_data = data[data["status"] == "Default"].copy()
    return good_data, bad_data, search
