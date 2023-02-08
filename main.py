import numpy as np
import pandas as pd
import xlrd

from utils import *


if __name__ == "__main__":
    table = pd.read_excel("asset/Simplified Hierachical Table.xlsx",
                          header=[0, 1],
                          index_col=[0, 1])

    table = revision_index(table)

    print(table.index)
    print(table)
    print(table[2018]["Spring"].loc['Stars']['KDA'])
    print(table[2018]["Spring"].loc['Wolves'])
