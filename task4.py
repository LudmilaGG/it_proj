import pandas as pd
import matplotlib.pyplot as plt
import datetime
import numpy as np
import settings

from utils import export_data, parse_date
from general import DBhandler

from sklearn.metrics import confusion_matrix, auc

db_connection = DBhandler(db_path=settings.DATABASE_PATH)

clients_scores = pd.read_sql_query("SELECT * FROM %s" % settings.CLIENT_SCORES_TABLE, db_connection.connection)

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

db_connection.execute_sql("DROP TABLE IF EXISTS %s" % settings.TOTAL_SCORES_TABLE)
clients_scores[['id', 'total_score']].set_index('id', drop=True).to_sql(settings.TOTAL_SCORES_TABLE, db_connection.connection)

export_data("client scores table", "total_scores.csv", clients_scores[['id', 'total_score']].to_csv)

# risk horizon

payments_df = pd.read_sql_query("SELECT * FROM %s" % settings.PAYMENT_TABLE, db_connection.connection)

payments_df.payment_date = payments_df.payment_date.apply(parse_date)

payments_df['overdue_days'] = -1

for contract in payments_df.contract_id.unique():
    temp_df = payments_df[payments_df.contract_id == contract].sort_values(['payment_date'])
    index_arr = temp_df.index.tolist()
    for i in range(1, len(index_arr)):
        prev_ind = index_arr[i - 1]
        ind = index_arr[i]
        if payments_df.loc[ind, 'amount_paid'] < payments_df.loc[ind, 'amount_due']:
            if payments_df.loc[prev_ind, 'overdue_days'] == -1:
                payments_df.loc[ind, 'overdue_days'] = 0
            else:
                new_ov_days = 30
                payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] + new_ov_days
        elif payments_df.loc[ind, 'amount_paid'] > payments_df.loc[ind, 'amount_due']:
            months_covered = int(round(payments_df.loc[ind, 'amount_paid'] / payments_df.loc[ind, 'amount_due'], 0))
            months_covered = months_covered - 1
            payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] - months_covered * 30
            if payments_df.loc[ind, 'overdue_days'] < 0:
                payments_df.loc[ind, 'overdue_days'] = -1  # Если погасил весь долг - нет просрочки

default_dates = payments_df[payments_df.overdue_days == 90].groupby(['contract_id'], as_index=False).agg({"payment_date": "max"})

contracts_df = pd.read_sql_query("SELECT * FROM %s" % settings.CONTRACT_TABLE, db_connection.connection, index_col='contract_id')
contracts_df.contract_date = contracts_df.contract_date.apply(parse_date)

payments_df['contract_date'] = np.nan
payments_df['id_number'] = 0
for ind in payments_df.index:
    row = payments_df.loc[ind]
    payments_df.loc[ind, 'contract_date'] = contracts_df.loc[row.contract_id].contract_date
    payments_df.loc[ind, 'id_number'] = contracts_df.loc[row.contract_id].id

payments_df['age'] = 0
for contract in payments_df.contract_id.unique():
    temp_df = payments_df[payments_df.contract_id == contract].sort_values(['payment_date'])
    index_arr = temp_df.index.tolist()
    contract_date = temp_df.contract_date.iloc[0]  # Берем первую дату договора из таблицы, т.к. они все одинаковые
    for i in range(0, len(index_arr)):
        ind = index_arr[i]
        curr_date = payments_df.loc[ind, 'payment_date']
        curr_age = (curr_date.year - contract_date.year) * 12 + (curr_date.month - contract_date.month)
        payments_df.loc[ind, 'age'] = curr_age

period_months = int(input("Please enter risk horizon (in months): "))

out_df = payments_df[payments_df.age == period_months][['contract_id',
                                                        'payment_date',
                                                        'contract_date',
                                                        'id_number',
                                                        'age']].reset_index(drop=True)  # Берем только нужные поля и "сбрасываем" индекс (начинаем с 0)

out_df['Default?'] = False

for contract in out_df.contract_id.unique():
    if contract in default_dates.contract_id.values:
        default_date = default_dates[default_dates.contract_id == contract].payment_date.iloc[0]
        current_date = out_df.loc[out_df.contract_id == contract, "payment_date"].iloc[0]
        if current_date >= default_date:
            out_df.loc[(out_df.contract_id == contract), "Default?"] = True

out_df_gb = out_df.groupby(['id_number'], as_index=False).agg({"age": "max", "Default?": "max"})

out_df_gb = out_df_gb[out_df_gb.id_number.isin(clients_scores.id.unique())]

out_df_gb.sort_values(['id_number'], inplace=True)

out_df_gb['score'] = out_df_gb.id_number.apply(lambda x: clients_scores.set_index(['id']).loc[x].total_score)

scores_arr = sorted(out_df_gb.score.unique())

x_plot = []
y_plot = []
y_true = out_df_gb['Default?'].astype(int).values
for threshold in ([0.0] + scores_arr + [100.0]):
    y_pred = (out_df_gb.score <= threshold).values.astype(int)
    conf_mx = confusion_matrix(y_true, y_pred)
    tpr = conf_mx[1, 1] / conf_mx[1].sum()
    fpr = conf_mx[0, 1] / conf_mx[0].sum()
    x_plot.append(fpr)
    y_plot.append(tpr)

roc_curve = plt.figure()
plt.plot(x_plot, y_plot, label='ROC curve (area = %0.2f)' % auc(x_plot, y_plot))
plt.plot([0, 1], [0, 1], '--r')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC curve')
plt.legend(loc="lower right")
plt.ion()
plt.show()

plt.pause(0.01)
input("Press [enter] to continue...")

export_data("roc plot", "roc_plot.png", roc_curve.savefig)
