from datetime import datetime
import os

"""
Name: DF_TO_CSV                            
Created by: Chris Wilson, Sam McVicar                   
Creation Date: 11/9/2020                                
Purpose: This takes a dataframe and dumps it to a CSV File                                                                              
"""


def data_write(df, loc, file, wmode):
    """
    This function outputs a dataframe to a csv.
    :param df: dataframe that is being passed to the function, this is the data for the csv
    :param loc: location of drive where the CSV will be dropped
    :param file: name of the file that is being dropped
    :param wmode: determines what to do with the file, w = overwrite, x = do not overwrite
    :return:
    """
    out_file = loc + '\\' + (file + ('_' + datetime.today().strftime("%Y%m%d")))+'.csv'
    if not os.path.exists(loc):
        os.mkdir(loc)
    try:
        df.to_csv(out_file, mode=wmode, index=False)
    except Exception as e:
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
