from hdbcli import dbapi
import pandas as pd
import os
import datetime
from dateutil.relativedelta import relativedelta

"""                            
Purpose: Exports data from a view, maniuplates the column names
and then exports it to a location based on column distinct values.
The export process is in CSV and will create the folder if it doesn't exist.                                                                
"""


def hana_df(username, password, environment, frm):
    """
    Connects to a HANA database to return a view
    :param username: DB user
    :param password: DB pass
    :param environment: DB env
    :param frm: DB view
    :return: Dataframe
    """
    prod_connection_han = ['SCHEMA', 00000, f'{username}', f'{password}']
    dev_connection_han = ['SCHEMA', 00000, f'{username}', f'{password}']
    connection_lam = (lambda e: prod_connection_han if e.upper() in ['Prod', 'Production'] else (
        dev_connection_han if e.upper() in ['Dev', 'Development'] else 'Invalid'))(environment)
    column_query_view = f'''SELECT "COLUMN_NAME"
    FROM "SCHEMA"."path.to.view/VIEW_COLUMN_DESCRIPTION"
    WHERE "SCHEMA_NAME" = 'SCHEMA' AND "VIEW_NAME" = '{frm}'
    ORDER BY POSITION;'''
    data_view = f'''SELECT * FROM "SCHEMA"."{frm}" '''
    connection_han = dbapi.connect(f'{connection_lam[0]}', connection_lam[1], f'{connection_lam[2]}',
                                   f'{connection_lam[3]}')
    cursor_han = connection_han.cursor()
    try:
        cursor_han.execute(column_query_view)
        columns = cursor_han.fetchall()
        try:
            cursor_han.execute(data_view)
            dta = cursor_han.fetchall()
            cursor_han.close()
            z_columns = [x[0] for x in columns]
            df = pd.DataFrame(data=dta, columns=z_columns)
            return df
        except Exception as e:
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
    cursor_han.close()


def df_normalize(result, i):
    """
    This renames columns based on their name, its getting the month based on todays date
    each of the columns reperesent a date period so rather than have them just say MONTH_0, MONTH_1, etc.
    It will say FEB-2021, MAR-2021, etc.
    :param result: dataframe
    :param i: range(18)
    :return: no return
    """
    M = (datetime.datetime.now() + relativedelta(months=i)).strftime("%B-%Y")
    result.rename(columns={f'Month_{i+1}': M}, inplace=True)


def output(x, result):
    """
    Preps the file location and calls the function to write the data
    :param x: Unique ID
    :param result: Data frame
    :return:
    """
    # TODO: Move this down into the data_write function, it's not really needed..

    location = f"\\Path\{x}"
    file_name = f'{x}_DATASET'
    try:
        data_write(result.loc[result["UNIQUE_ID"] == x], location, file_name, 'w')
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)


def data_write(df, loc, file, wmode):
    """
    Exports the data to a drive location
    :param df: dataframe
    :param loc: output location
    :param file: filename
    :param wmode: write mode
    :return: no output
    """
    out_file = loc + '\\' + file + '.csv'
    if not os.path.exists(loc):
        os.mkdir(loc)
    try:
        df.to_csv(out_file, mode=wmode, index=False)
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)


if __name__ == '__main__':
    """
        Main class, set username/password/environment/frm
    """
    username = os.getenv('Username')
    password = os.getenv('Password')
    environment = 'Development'
    frm = 'path.to.view/The_data_set'
    month_offsets = range(18)
    result = hana_df(username, password, environment, frm)
    [df_normalize(result, i) for i in month_offsets]
    Unique_ID_List = result.UNIQUE_ID.unique()
    [output(x, result.loc[result["UNIQUE_ID"] == x]) for x in supplier_list]
