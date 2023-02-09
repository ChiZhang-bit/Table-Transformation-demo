import numpy as np
import pandas as pd
import xlrd
import os

from utils import *
from DataInsight import *

if __name__ == "__main__":
    table_name = "Hierachical Table2.xlsx"
    table = pd.read_excel(os.path.join("asset", table_name),
                          header=[0, 1],
                          index_col=[0, 1, 2])

    table = revision_index(table)

    print(table)

    tableinsight = TableInsight(table, table_name)

    tableinsight.single_outlier(1, 2)
    tableinsight.single_trend(1, 2)

    # print(table.index)
    # print(table.keys())
    # print(table)
    # print(table["Spring"].loc['WuHan Estar']['KDA'])
    # print(table[2018]["Spring"].loc['ChongQing Wolves'])
