import pandas as pd
import numpy as np

class TableInsight(object):
    def __init__(self, table: pd.DataFrame, table_id: str):
        self.table = table
        self.table_id = table_id
        self.top_header = table.keys()
        self.left_header = table.index
        self.top_level = len(self.top_header[0])
        self.left_level = len(self.left_header[0])

    def __str__(self):
        return f"The table_id is {self.table_id}\n"\
               f"The table top_header is {self.top_header}, level:{self.top_level}\n"\
               f"The table left_header is {self.left_header}, level:{self.left_level}\n"\
               f"The table is {self.table}"

    def single_outlier(self, row: int, column: int):
        """
        假定只有评判一个single_cell的outlier, 评估其异常程度
        :return: 异常程度
        """
        # Step1: find current cell location
        left_location = self.left_header[row]
        top_location = self.top_header[column]
        print(left_location)
        print(top_location)
        print(self.table.iloc[row, column])

        # Step2: 根据层次找到与其相关的单元格
        data = self.table[top_location]
        print(data.xs("KDA", level=1))
