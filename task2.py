import sqlite3
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt

conn = sqlite3.connect("database/origin.db")
cur = conn.cursor()

payments_df = pd.read_sql_query("SELECT * FROM payment", conn)

payments_df.payment_date = payments_df.payment_date.apply(lambda x: datetime.datetime.strptime(x, "%m.%d.%Y").date())

s_dates = payments_df.groupby(['contract_id'], as_index=False).agg({'payment_date': 'min'})

s_dates['amount_due'] = np.nan
s_dates['amount_paid'] = np.nan

for ind in s_dates.index:
    row = s_dates.loc[ind]
    t_df = payments_df[(payments_df.contract_id == row.contract_id) & (payments_df.payment_date == row.payment_date)]
    s_dates.loc[ind, 'amount_due'] = t_df.amount_due.iloc[0]
    s_dates.loc[ind, 'amount_paid'] = t_df.amount_paid.iloc[0]

if np.any(s_dates.amount_paid - s_dates.amount_due < 0):
    mb_zero_default = s_dates[s_dates.amount_paid - s_dates.amount_due < 0].contract_id.unique()
    print("There may be overdues in the first period! Check contracts:", mb_zero_default)

# Заполняем количество дней просрочки

payments_df['overdue_days'] = -1  # Ставим -1, там, где не было просрочки (потом уберем), а 0 будем ставить там, где должен был быть платеж, но не произошёл

for contract in payments_df.contract_id.unique():
    temp_df = payments_df[payments_df.contract_id == contract].sort_values(['payment_date'])
    # Получаем массив индексов, т.к. индексация в "частях" pandas.DataFrame остаётся как и в исходном
    index_arr = temp_df.index.tolist()
    for i in range(1, len(index_arr)):  # Берем индексы с 1, т.к. в 0 всегда просрочка 0 (первый платеж)
        prev_ind = index_arr[i - 1]
        ind = index_arr[i]
        if payments_df.loc[ind, 'amount_paid'] < payments_df.loc[ind, 'amount_due']:  # Платеж не поступил или был меньше
            if payments_df.loc[prev_ind, 'overdue_days'] == -1:  # Не было просрочки
                payments_df.loc[ind, 'overdue_days'] = 0  # С текущей даты пошла просрочка, но пока что 0
            else:
                #  Для простоты будем просто добавлять 30, т.к. платежи ровно через 1 мес.
                new_ov_days = 30
                payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] + new_ov_days  # Складываем прошлую просрочку с текущей
        elif payments_df.loc[ind, 'amount_paid'] > payments_df.loc[ind, 'amount_due']:
            months_covered = int(round(payments_df.loc[ind, 'amount_paid'] / payments_df.loc[ind, 'amount_due'], 0))  # Считаем, за сколько месяцев оплатил
            months_covered = months_covered - 1  # Вычитаем 1 месяца из погашения, т.к. мы не прибавили 30 дней за текущий месяц
            # Вычитаем из просрочки количество "погашенных" просроченных платежей
            # В случае более сложного начисления (с процентами, неравномерные платежи) надо использовать логику сложнее
            payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] - months_covered * 30
            if payments_df.loc[ind, 'overdue_days'] < 0:
                payments_df.loc[ind, 'overdue_days'] = -1  # Если погасил весь долг - нет просрочки

# Берем макс. дату выхода в просрочку 90+, т.к. вдруг клиент гасил просрочку 90 и возрашался в 60
default_dates = payments_df[payments_df.overdue_days == 90].groupby(['contract_id'], as_index=False).agg({"payment_date": "max"})

print(default_dates.rename({"contract_id": "Contract Number", "payment_date": "Default Date"}, axis=1))


contracts_df = pd.read_sql_query("SELECT * FROM contract", conn, index_col='contract_id')
contracts_df.contract_date = contracts_df.contract_date.apply(lambda x: datetime.datetime.strptime(x, "%m.%d.%Y").date())

# Определяем дату договора и заёмщика
payments_df['contract_date'] = np.nan
payments_df['id_number'] = 0
for ind in payments_df.index:
    row = payments_df.loc[ind]
    payments_df.loc[ind, 'contract_date'] = contracts_df.loc[row.contract_id].contract_date
    payments_df.loc[ind, 'id_number'] = contracts_df.loc[row.contract_id].id

# Определяем "возраст" договора в месяцах на каждую дату
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

out_df_gb = out_df.groupby(['id_number'], as_index=False).agg({"age": "max", "Default?": "max"}).rename({"age": "Age (months)"}, axis=1)
print(out_df_gb)

# Выбираем нужные данные о клиентах
applications_df = pd.read_sql_query("""
SELECT
    *
FROM
    profile
""", conn, index_col='id')

# Убираем ненужные колонки
applications_df.drop(["issue_date"], axis=1, inplace=True)
applications_df.birth = applications_df.birth.apply(lambda x: datetime.datetime.strptime(x, "%m.%d.%Y").date())

# Убираем клиента, по которому мало данных

if 100076 in applications_df.index:
    applications_df.drop([100076], inplace=True)

# Заполняем пустые поля
applications_df.fillna(0.0, inplace=True)

applications_df['default'] = 0

applications_df.loc[applications_df.index.isin(out_df_gb[out_df_gb['Default?']].id_number.unique()), "default"] = 1

applications_df['age'] = (datetime.date.today() - applications_df.birth).apply(lambda x: x.days // 365)

applications_df.drop(['birth'], axis=1, inplace=True)

applications_df.income = pd.qcut(applications_df.income, 5)


def get_age_of_car_category(age):
    if age == 0:
        return '0'
    elif age <= 3:
        return '<=3'
    else:
        return '>3'


applications_df.age_of_car = applications_df.age_of_car.apply(get_age_of_car_category)

applications_df.age = pd.qcut(applications_df.age, 5)

quantilized_columns = ['income', 'age', 'age_of_car']

categorical_columns = ['gender',
                       'employed_by',
                       'education',
                       'marital_status',
                       'position',
                       'income_type',
                       'housing',
                       'house_ownership',
                       'children',
                       'family',
                       ]

choices = dict(zip(range(len(categorical_columns + quantilized_columns)), sorted(categorical_columns + quantilized_columns)))

for i in choices.keys():
    print("%d: %s" % (i, choices[i]))

column_choice = choices.get(int(input("Please choose column (default=0): ")), 0)

temp_df = applications_df[[column_choice, 'default']].copy()

temp_df['event'] = temp_df.default
temp_df['non_event'] = 1 - temp_df.default

df_gb = temp_df.groupby([column_choice]).agg({"default": "sum",
                                         "event": lambda x: x.sum() / temp_df.event.sum(),
                                         "non_event": lambda x: x.sum() / temp_df.non_event.sum()})

df_gb['woe'] = np.log(np.clip(df_gb.event / df_gb.non_event, 0.001, np.inf))

df_gb['IV'] = (df_gb.event - df_gb.non_event) * df_gb.woe

df_gb.index = df_gb.index.astype(str)

plt.figure(figsize=(12, 7));
plt.scatter(list(range(df_gb.shape[0])), df_gb.woe)
plt.grid()
plt.xticks(list(range(df_gb.shape[0])), df_gb.index)
plt.ylabel("WOE", fontsize=14)
plt.xlabel(column_choice, fontsize=14)
plt.title("WOE plot", fontsize=16)
plt.ion()
plt.show()

plt.figure(figsize=(12, 7));
plt.scatter(list(range(df_gb.shape[0])), df_gb.IV)
plt.grid()
plt.xticks(list(range(df_gb.shape[0])), df_gb.index)
plt.ylabel("IV", fontsize=14)
plt.xlabel(column_choice, fontsize=14)
plt.title("Information Value plot", fontsize=16)
plt.ion()
plt.show()

df_gb.loc['SUM'] = df_gb.sum()

print(df_gb)

plt.pause(0.001)
input("Press [enter] to continue...")
