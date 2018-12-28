import os

from general import DBhandler, create_profile, create_contract, create_payments

ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
DATABASE_DIR = os.path.join(ROOT_DIR, "database")
DATABASE_NAME = "origin.db"
DATABASE_PATH = os.path.join(DATABASE_DIR, DATABASE_NAME)
PAYMENTS_PATH = os.path.join(DATA_DIR, 'payments.xls')

if not os.path.exists(DATA_DIR):
    raise Exception("DATA_DIR is missing")
if not os.path.exists(DATABASE_DIR):
    os.makedirs(DATABASE_DIR)

db_connection = DBhandler(db_path=DATABASE_PATH)
create_profile(connection=db_connection, data_path=DATA_DIR)
create_contract(connection=db_connection, data_path=DATA_DIR)
create_payments(connection=db_connection, data_path=PAYMENTS_PATH)

# CREATING MAP
db_connection.create_map(values_map={"Female": 1, "Male": 0}, target_table="profile", target_column="gender")
db_connection.create_map(values_map={"Secondary / secondary special": 0,
                                     "Incomplete higher": 1,
                                     "Higher education": 2,
                                     "": 3}, target_table="profile", target_column="education")
db_connection.create_map(values_map={"Separated": 0,
                                     "Single / not married": 1,
                                     "Widow": 2,
                                     "Married": 3,
                                     "Civil marriage": 4,
                                     '': 5}, target_table="profile", target_column="marital_status")
db_connection.create_map(values_map={"House / apartment": 0,
                                     "Municipal apartment": 1,
                                     "Rented apartment": 2,
                                     "With parents": 3,
                                     '': 4}, target_table="profile", target_column="housing")
db_connection.create_map(values_map={"Cash loans": 0,
                                     "Revolving loans": 1}, target_table="contract", target_column="type")
db_connection.create_map(values_map={"Commercial associate": 0,
                                     "Working": 1,
                                     "State servant": 2,
                                     "Pensioner": 3,
                                     "": 4}, target_table="profile", target_column="income_type")

db_connection.create_map(values_map={"Services": 0,
                                     "Transport: type 2": 1,
                                     "Business Entity Type 3": 2,
                                     "Medicine": 3,
                                     "University": 4,
                                     "Housing": 5,
                                     "Government": 6,
                                     "Other": 7,
                                     "Self-employed": 8,
                                     "XNA": 9,
                                     "School": 10,
                                     "Kindergarten": 11,
                                     "Business Entity Type 2": 12,
                                     "Electricity": 13,
                                     "Transport: type 4": 14,
                                     "Industry: type 11": 15,
                                     "Trade: type 7": 16,
                                     "Trade: type 2": 17,
                                     "Military": 18,
                                     "Industry: type 1": 19,
                                     "Security": 20,
                                     "Religion": 21,
                                     "Security Ministries": 22,
                                     "Emergency": 23,
                                     "Transport: type 3": 24,
                                     "Construction": 25,
                                     }, target_table='profile', target_column='employed_by')

db_connection.create_map(values_map={'Managers': 0,
                                     'Laborers': 1,
                                     '<undefined>': 2,
                                     'Core staff': 3,
                                     'Private service staff': 4,
                                     'Drivers': 5,
                                     'Medicine staff': 6,
                                     'Sales staff': 7,
                                     'Accountants': 8,
                                     'Security staff': 9,
                                     'Cleaning staff': 10,
                                     'Cooking staff': 11,
                                     }, target_table='profile', target_column='position')

# JOIN TABLES
db_connection.join_tables(table1="profile", table2="contract", left_key="id", right_key="id")
db_connection.join_tables(table1="profile_contract", table2="payment", left_key="contract_id", right_key="contract_id")
