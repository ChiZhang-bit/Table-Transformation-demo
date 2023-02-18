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

    # table.sort_index(inplace=True, axis=1, key=lambda x: sort_func(x))
    # print(table)

    tableinsight = TableInsight(table, table_name)
    # loc = [['Mid'], [2018], ["Spring", "Autumn"], ["KDA"]]
    # left_loc, top_loc = tableinsight.revision_loc_list(loc)
    # print(left_loc, top_loc)
    # input()

    # print(tableinsight.table)
    # tableinsight.transform_top(0, 1)
    # print(tableinsight.table)

    # left_loc = [["Nanjing Hero", "WuHan Estar"], "*", ["KDA"]]
    # top_loc = ["*", ["Spring", "Summer", "Autumn"]]

    # print(tableinsight.normal_loc_func(left_loc, top_loc))
    # input()
    # print(tableinsight.data_location(left_loc, top_loc))
    # input()

    # tableinsight.merge_transformation_by_headers(left_loc, top_loc)
    # print(tableinsight.table)
    # tableinsight.explortory_tree(left_loc, top_loc)

    # tableinsight.find_list_by_index_num([0, 1], [0, 1, 2, 3])
    # input()

    # tableinsight.single_outlier([1], [2])
    # tableinsight.single_trend([1], [2])
    # tableinsight.single_max_min_imum([1], [2])

    # tableinsight.block_trend(left_loc=left_loc, top_loc=top_loc)
    # tableinsight.block_correlation(left_loc, top_loc)

    rows = [0, 1, 2, 3, 4]
    columns = [0, 1, 2, 3, 5, 6]
    left_loc = [["ChongQing Wolves"], ["Jungle"], ["KDA"]]
    top_loc = [[2018], ["Spring"]]
    # tableinsight.decision_transformation_way(left_loc=left_loc, top_loc=top_loc)
    # print(tableinsight.table)
    tableinsight.explortory_tree(left_loc, top_loc)

    # tableinsight.block_trend(left_loc, top_loc)
