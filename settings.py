import os

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

PROFILE_TABLE = 'profile'
CONTRACT_TABLE = 'contract'
PAYMENT_TABLE = 'payment'
CLIENT_SCORES_TABLE = 'client_scores'
TOTAL_SCORES_TABLE = 'total_scores'
VALUES_MAP_TABLE = 'values_map'
