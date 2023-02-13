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
    # list1 = []
    # a = table.xs("ChongQing Wolves", level=0, axis=0, drop_level=False)
    # list1.append(a)
    # b = table.xs("WuHan Estar", level=0, axis=0, drop_level=False)
    # c = table.xs("Nanjing Hero", level=0, axis=0, drop_level=False)
    # 这里大概知道该怎么改筛选数据的部分了
    # list1.append(b)
    # list1.append(c)
    # print(pd.concat(list1))

    tableinsight = TableInsight(table, table_name)

    # print(tableinsight.table)
    # tableinsight.transform_top(0, 1)
    # print(tableinsight.table)

    left_loc = [["Nanjing Hero", "WuHan Estar"], ["Mid", "Jungle"], ["KDA"]]
    top_loc = ["*", ["Spring", "Summer"]]
    print(tableinsight.data_location(left_loc, top_loc))
    # tableinsight.merge_transformation_by_headers(left_loc, top_loc)
    # print(tableinsight.table)
    # tableinsight.explortory_tree(left_loc, top_loc)
    input()

    tableinsight.single_outlier(1, 2)
    tableinsight.single_trend(1, 2)
    tableinsight.single_max_min_imum(1, 2)

    # print(table["Spring"].loc['WuHan Estar']['KDA'])
    # print(table[2018]["Spring"].loc['ChongQing Wolves'])

    rows = [0, 1, 2, 3, 4, 5, 6, 7]
    columns = [0, 1, 2, 3]
    tableinsight.block_trend(rows, columns)
