import numpy as np
import pandas as pd
import xlrd


def revision_index(hierarchi_table: pd.DataFrame):
    """
    修改层次表的索引，取消NaN值
    :param hierarchi_table:
    :return: Dataframe
    """
    revision_index = []
    for i in hierarchi_table.index:
        if np.nan in i:
            newlist = []
            for j in range(len(i)):
                if i[j] is np.nan:
                    newlist.append(temp[j])
                else:
                    newlist.append(i[j])
            revision_index.append(tuple(newlist))
        else:
            temp = i
            revision_index.append(i)
    hierarchi_table.index = pd.MultiIndex.from_tuples(revision_index)
    hierarchi_table.index.names = range(len(revision_index[0]))
    return hierarchi_table
