import pandas as pd
import numpy as np
import time
from dask import dataframe as ddf


def create_csv(file):
    """
    Generates a Large CSV File, the file size is around 1gb. This is good for playing with large files, if you need
    a larger file change the size=(10000000) to 40000000 and pd.util.testing.rands_array(5, 10000000) to
    pd.util.testing.rands_array(5, 40000000) this will result in a file around 5.22gb
    :param file: location of where you want to generate the file, see the path below in main
    :return: No return
    """
    df = pd.DataFrame(data=np.random.randint(99999, 99999999, size=(10000000, 14)),
                      columns=['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14'])
    df['C15'] = pd.util.testing.rands_array(5, 10000000)
    df.to_csv(file)


def panda_read_csv(file):
    """
    Reads the CSV file that you generated, this is done using Pandas. You could brute force this process by using
    this approach but its SLOW. You throw more resources at your machine but its still SLOW. Not much of a trade off
    right?
    :param file: location of where you want to generate the file, see the path below in main
    :return: Time (end - start)
    """
    start = time.time()
    df = pd.read_csv(file)
    end = time.time()
    result = (end - start)
    return result


def panda_read_csv_chunks(file):
    """
    Reads the CSV file that you generated, this is done using Pandas with Chunks. The idea is to not brute force the
    read but rather chunk it up into consumable sections and read those. At the end of the process you can use
    pd.concat() to concat your dataframe.
    :param file: location of where you want to generate the file, see the path below in main
    :return: Time (end - start)
    """
    start = time.time()
    df = pd.read_csv(file, chunksize=1000000)
    end = time.time()
    result = (end - start)
    pd_df = pd.concat(df)
    return result


def dask_read_csv(file):
    """
    Dask is supposed to be a faster way to read the data from large CSV files. The idea is that it utilizes multiple
    CPU cores and chunks the data up and processes it in parallel. DASK can be used for things other than a DF, it has
    support for array and scikit-learn libraries to take advantage of parallelism.
    :param file:
    :return:
    """
    start = time.time()
    dask_df = ddf.read_csv(file)
    end = time.time()
    result = (end - start)
    return result


if __name__ == '__main__':
    file_loc = r"E:\desktop\Portfolio\Python - For Fun\Reading large CSV\huge_data.csv"
    create_csv(file_loc)
    r1 = panda_read_csv(file_loc)
    r2 = panda_read_csv_chunks(file_loc)
    r3 = dask_read_csv(file_loc)
    print(f"Pandas Read CSV:{r1} secs")
    print(f"Pandas Read CSV w/Chunks :{r2} secs")
    print(f"Dask Read CSV:{r3} secs")
