from hdbcli import dbapi
import pandas as pd


def hana_df(username, password, environment, schema, frm, obj_type):
    """
    This function takes in the below params to generate a dataframe with the data from the view
    or table selected with column names included
    :param username: HANA Username
    :param password: HANA Password
    :param environment: Dev, QA, Prod
    :param schema: Schema_Name   NOTE: This is not needed for views, just put ''
    :param frm: View or Table name. Example, view: 'path.to.view/VIEW_COLUMN_DESCRIPTION  table: Table_name
    :param obj_type: Table or View
    :return: The dataframe
    """
    prod_connection_han = ['DB1', 00000, '{}'.format(username), '{}'.format(password)]
    dev_connection_han = ['DB2', 00000, '{}'.format(username), '{}'.format(password)]
    connection_lam = (lambda e: prod_connection_han if e.upper() in ['Prod', 'Production'] else (
        dev_connection_han if e.upper() in ['Dev', 'Development'] else 'Invalid'))(environment)

    column_query_table = '''SELECT "COLUMN_NAME"
    FROM "SCHEMA"."path.to.view/TABLE_COLUMN_DESCRIPTION"
    WHERE SCHEMA_NAME = '{}' AND TABLE_NAME = '{}'
    ORDER BY POSITION;'''.format(schema, frm)
    column_query_view = '''SELECT "COLUMN_NAME"
    FROM "SCHEMA"."path.to.view/VIEW_COLUMN_DESCRIPTION"
    WHERE "SCHEMA_NAME" = '_SYS_BIC' AND "VIEW_NAME" = '{}'
    ORDER BY POSITION;'''.format(frm)
    type_lam = (lambda e: column_query_table if e.upper() == 'TABLE' else (column_query_view if e.upper() == 'VIEW'
                                                                           else 'Invalid'))(obj_type)

    connection_han_col = dbapi.connect('{}'.format(connection_lam[0]), int('{}'.format(connection_lam[1])),
                                       '{}'.format(connection_lam[2]), '{}'.format(connection_lam[3]))
    cursor_han_col = connection_han_col.cursor()
    cursor_han_col.execute(type_lam)
    columns = cursor_han_col.fetchall()
    z_columns = [x[0] for x in columns]
    connection_han_col.close()

    data_table = '''SELECT * FROM {}.{}'''.format(schema, frm)
    data_view = '''SELECT * FROM "_SYS_BIC"."{}" '''.format(frm)
    data_lam = (lambda e: data_table if e.upper() == 'TABLE' else (data_view if e.upper() == 'VIEW'
                                                                   else 'Invalid'))(obj_type)

    connection_han_data = dbapi.connect('{}'.format(connection_lam[0]), int('{}'.format(connection_lam[1])),
                                        '{}'.format(connection_lam[2]), '{}'.format(connection_lam[3]))

    cursor_han_data = connection_han_data.cursor()
    cursor_han_data.execute(data_lam)
    dta = cursor_han_data.fetchall()
    df = pd.DataFrame(data=dta, columns=z_columns)
    connection_han_data.close()
    return df

