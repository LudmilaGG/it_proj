import sqlite3
import pandas as pd
import os

conn = sqlite3.connect("database/origin.db")
cur = conn.cursor()

clients_scores = pd.read_sql_query("SELECT * FROM client_scores", conn)

score_weights = {
    "age": 0.4919,
    "age_of_car": 0.3890,
    "education": 0.0389,
    "employed_by": 1.0320,
    "family": 0.3844,
    "house_ownership": 0.0023,
    "housing": 0.1372,
    "income": 0.4485,
    "income_type": 0.2288,
    "marital_status": 0.1281,
    "position": 0.7185,
}

clients_scores['total_score'] = 0
for ind in clients_scores.index:
    row = clients_scores.loc[ind]
    temp_scores = dict()
    for column_name in score_weights.keys():
        temp_scores[column_name] = row[column_name] * score_weights[column_name]
    clients_scores.loc[ind, 'total_score'] = sum(temp_scores.values())

print(clients_scores[['id', 'total_score']])

cur.execute("DROP TABLE IF EXISTS total_scores")
clients_scores[['id', 'total_score']].set_index('id', drop=True).to_sql("total_scores", conn)

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

    clients_scores[['id', 'total_score']].to_csv(os.path.join(output_path, 'total_scores.csv'))
    print("File was saved to %s !" % os.path.join(output_path, 'total_scores.csv'))
