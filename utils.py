import sqlite3
conn = sqlite3.connect("database/origin.db")
cur = conn.cursor()


def convert_data_from_categorical(values, column_name):
    new_values = []
    for value in values:
        cur.execute("SELECT key FROM values_map WHERE column_name = '%s' AND value = %d" % (column_name, int(value)))
        new_values.append(cur.fetchone()[0])
    return new_values
