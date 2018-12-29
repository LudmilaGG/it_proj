import os
import settings
from general import DBhandler
import datetime

db_connection = DBhandler(db_path=settings.DATABASE_PATH)


def convert_data_from_categorical(values, column_name):
    new_values = []
    for value in values:
        db_connection.cursor.execute("SELECT key FROM %s WHERE column_name = '%s' AND value = %d" % (settings.VALUES_MAP_TABLE, column_name, int(value)))
        new_values.append(db_connection.cursor.fetchone()[0])
    return new_values


def export_data(export_name, file_name, function_to_call, **params):
    save_data = input("Do you want to save %s? [No]: " % export_name).lower()
    if len(save_data) == 0:
        save_data = 'no'
    while save_data not in ['n', 'no', 'y', 'yes']:
        save_data = input("Do you want to save %s? [No]: " % export_name).lower()
        if len(save_data) == 0:
            save_data = 'no'

    save_data = True if save_data in ['y', 'yes'] else False

    if save_data:
        output_path = input("Enter path to output (Or leave blank for current folder): ")

        if not len(output_path):
            output_path = './'

        while not os.path.exists(output_path):
            print("Path doesn't exist!")
            output_path = input("Enter path to output (Or leave blank for current folder): ")

        function_to_call(os.path.join(output_path, file_name), **params)
        print("File was saved to %s !" % os.path.join(output_path, file_name))


def parse_date(str_date):
    return datetime.datetime.strptime(str_date, "%m.%d.%Y").date()
