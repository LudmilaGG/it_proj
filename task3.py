import sqlite3
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import os

from utils import convert_data_from_categorical


def age_category(age):
    if 18 <= age < 25:
        return '[18, 25)'
    elif 25 <= age < 35:
        return '[25, 35)'
    elif 35 <= age < 55:
        return '[35, 55)'
    elif age != age:
        return 'None'
    else:
        return '[55, inf)'


def income_category(income):
    if isinstance(income, str) and not len(income):
        income = float('nan')
    else:
        income = float(income)
    if income < 40000:
        return '[0-40000)'
    elif 40000 <= income < 100000:
        return '[40000, 100000)'
    elif 100000 <= income < 200000:
        return '[100000, 200000)'
    elif 200000 <= income < 500000:
        return '[200000, 500000)'
    elif income != income:
        return 'None'
    else:
        return '[500000, inf)'


def age_of_car_category(age_of_car):
    if isinstance(age_of_car, str) and not len(age_of_car):
        age_of_car = float('nan')
    else:
        age_of_car = float(age_of_car)
    if age_of_car < 1:
        return '[0, 1)'
    elif 1 <= age_of_car < 3:
        return '[1, 3)'
    elif age_of_car != age_of_car:
        return 'None'
    else:
        return '[3, inf)'


def score_client(client):
    def age_score(age):
        if age == '[18, 25)':
            return 10
        elif age == '[25, 35)':
            return 20
        elif age == '[35, 55)':
            return 25
        elif age == 'None':
            return 0
        elif age == '[55, inf)':
            return 15

    def family_score(family):
        if family == 1:
            return 10
        elif family == 2:
            return 15
        elif family == 3:
            return 25
        elif family == 4:
            return 20
        else:
            return 0

    def income_score(income):
        if income == '[0-40000)':
            return 5
        elif income == '[40000, 100000)':
            return 10
        elif income == '[100000, 200000)':
            return 15
        elif income == '[200000, 500000)':
            return 20
        elif income == 'None':
            return 0
        elif income == '[500000, inf)':
            return 25

    def house_ownership_score(house_ownership):
        if house_ownership:
            return 25
        else:
            return 0

    def age_of_car_score(age_of_car):
            if age_of_car == '[0, 1)':
                return 25
            elif age_of_car == '[1, 3)':
                return 15
            elif age_of_car == 'None':
                return 0
            elif age_of_car == '[3, inf)':
                return 5

    def employed_by_score(employed_by):
        employed_by_score_dict = {
            "Business Entity Type 3": 25,
            "Business Entity Type 2": 25,
            "Government": 20,
            "Military": 5,
            "Security Ministries": 15,
            "Emergency": 5,
            "Security": 5,
            "Construction": 5,
            "Electricity": 5,
            "XNA": 0,
            "Other": 0,
        }
        return employed_by_score_dict.get(employed_by, 10)

    def education_score(education):
        if education == "Higher education":
            return 25
        elif education == "Secondary / secondary special":
            return 10
        elif education == "Incomplete higher":
            return 20
        else:
            return 0

    def marital_status_score(marital_status):
        if marital_status == "Married":
            return 25
        elif marital_status == "Single / not married":
            return 10
        elif marital_status == "Civil marriage":
            return 15
        elif marital_status == "Widow":
            return 5
        elif marital_status == "Separated":
            return 5
        else:
            return 0

    def position_score(position):
        if position == "Core staff":
            return 25
        elif position == "Accountants":
            return 25
        elif position == "Managers":
            return 25
        elif position == "Security staff":
            return 10
        elif position == "<undefined>":
            return 0
        else:
            return 15

    def income_type_score(income_type):
        if income_type == "State servant":
            return 15
        if income_type == "Working":
            return 10
        if income_type == "Commercial associate":
            return 25
        if income_type == "Pensioner":
            return 5
        else:
            return 0

    def housing_score(housing):
        if housing == "House / apartment":
            return 25
        elif housing == "Rented apartment":
            return 15
        elif housing == "With parents":
            return 10
        else:
            return 0

    score_dict = dict()
    score_dict['age'] = age_score(client.age)
    score_dict['family'] = family_score(client.family)
    score_dict['income'] = income_score(client.income)
    score_dict['house_ownership'] = house_ownership_score(client.house_ownership)
    score_dict['age_of_car'] = age_of_car_score(client.age_of_car)
    score_dict['employed_by'] = employed_by_score(client.employed_by)
    score_dict['education'] = education_score(client.education)
    score_dict['marital_status'] = marital_status_score(client.marital_status)
    score_dict['position'] = position_score(client.position)
    score_dict['income_type'] = income_type_score(client.income_type)
    score_dict['housing'] = housing_score(client.housing)
    return score_dict


conn = sqlite3.connect("database/origin.db")
cur = conn.cursor()

profile_data = pd.read_sql_query("SELECT * FROM profile", conn)
profile_data.birth = profile_data.birth.apply(lambda x: datetime.datetime.strptime(x, "%m.%d.%Y").date())
profile_data['age'] = profile_data.birth.apply(lambda x: (datetime.date.today() - x).days // 365)

used_columns = ['employed_by',
                'education',
                'marital_status',
                'position',
                'income_type',
                'housing']

for column in used_columns:
    profile_data[column] = convert_data_from_categorical(profile_data[column].values, column)

profile_data['age'] = profile_data.age.apply(age_category)
profile_data['income'] = profile_data.income.apply(income_category)
profile_data['age_of_car'] = profile_data.age_of_car.apply(age_of_car_category)

score_table = pd.DataFrame([], columns=['id',
                                        'age',
                                        'family',
                                        'income',
                                        'house_ownership',
                                        'age_of_car',
                                        'employed_by',
                                        'education',
                                        'marital_status',
                                        'position',
                                        'income_type',
                                        'housing',
                                        ])

curr_pos = 0
for i in range(len(profile_data)):
    client = profile_data.iloc[i]
    score_dict = score_client(client)
    score_table.loc[curr_pos] = [
        client.id,
        score_dict['age'],
        score_dict['family'],
        score_dict['income'],
        score_dict['house_ownership'],
        score_dict['age_of_car'],
        score_dict['employed_by'],
        score_dict['education'],
        score_dict['marital_status'],
        score_dict['position'],
        score_dict['income_type'],
        score_dict['housing'],
    ]
    curr_pos = curr_pos + 1

print(score_table)

cur.execute("DROP TABLE IF EXISTS client_scores")
score_table.set_index('id', drop=True).to_sql("client_scores", conn)

save_tables = input("Do you want to save table? [No]: ").lower()
if len(save_tables) == 0:
    save_tables = 'no'
while save_tables not in ['n', 'no', 'y', 'yes']:
    save_tables = input("Do you want to save table? [No]: ").lower()
    if len(save_tables) == 0:
        save_tables = 'no'

save_tables = True if save_tables in ['y', 'yes'] else False

if save_tables:
    output_path = input("Enter path to output (Or leave blank for current folder): ")

    if not len(output_path):
        output_path = './'

    while not os.path.exists(output_path):
        print("Path doesn't exist!")
        output_path = input("Enter path to output (Or leave blank for current folder): ")

    profile_data[['id', 'score']].to_csv(os.path.join(output_path, 'clients_scores.csv'))
    print("File was saved to %s !" % os.path.join(output_path, 'clients_scores.csv'))
