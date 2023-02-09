import numpy as np

from DataInsight.utils import *


class TableInsight(object):
    def __init__(self, table: pd.DataFrame, table_id: str):
        self.table = table
        self.table_id = table_id
        self.top_header = table.keys()
        self.left_header = table.index
        self.top_level = len(self.top_header[0])
        self.left_level = len(self.left_header[0])

    def __str__(self):
        return f"The table_id is {self.table_id}\n" \
               f"The table top_header is {self.top_header}, level:{self.top_level}\n" \
               f"The table left_header is {self.left_header}, level:{self.left_level}\n" \
               f"The table is {self.table}"

    def _single_related_data(self, row: int, column: int):
        left_location = self.left_header[row]
        top_location = self.top_header[column]
        # print(left_location)
        # print(top_location)
        related_data = []
        for i in range(self.left_level):
            data = self.table[top_location]
            for j in range(self.left_level):
                if i != j:
                    data = data.xs(left_location[j], level=j, axis=0)
            related_data.append(data)

        for i in range(self.top_level):
            data = self.table.loc[left_location]
            for j in range(self.top_level):
                if i != j:
                    data = data.xs(top_location[j], level=j, axis=0)
            related_data.append(data)
        return related_data

    def single_outlier(self, row: int, column: int):
        """
        假定只有一个单元格， 评判一个single_cell的outlier, 评估其异常程度
        :return: 异常程度
        """
        # Step1: find current cell location
        cell_value = self.table.iloc[row, column]

        # Step2: 根据层次找到与其相关的单元格，先找LEFT，再找TOP，
        # 顺序如下： 左侧表头1-n, 顶侧表头1-m
        related_data = self._single_related_data(row, column)
        # Step3: 根据不同的数据列使用异常值评估方法来量化异常度
        # 这里使用 1.5IQR Rule & three_sigma 来判断异常值
        outliers = []
        for i in related_data:
            if len(i) >= 20:
                outliers.append(three_sigma(i, cell_value))
            else:
                outliers.append(iqr_rule(i, cell_value))

        # Step4: 根据outliers中最大的位置的下标返回变换规则
        # print(outliers)
        if max(outliers) == 0:
            print("No related outlier!")
        else:
            outlier_index = outliers.index(max(outliers))
            if outlier_index <= self.left_level - 1:
                print(f"将左侧的第{outlier_index}层移到最右层")
            else:
                print(f"将顶部的第{outlier_index - self.left_level}层移到最下层")

    def single_trend(self, row: int, column: int):
        """
        假定只有一个单元格，评判一个single_cell的outlier, 评估其异常程度
        :param row:
        :param column:
        :return:
        """
        # Step1: 根据层次找到与其相关的单元格，先找LEFT，再找TOP，
        # 顺序如下： 左侧表头1-n, 顶侧表头1-m
        related_data = self._single_related_data(row, column)

        # Step2: 根据不同的数据列使用趋势评估方法来量化上升/下降的趋势
        trends = []
        for i in related_data:
            if len(i) >= 3:
                trends.append(trendline(i))
            else:
                trends.append((0, 0))

        # Step3: 根据相关系数>=0.8 (强相关的判定)，判断数据趋势的变换是否明显
        max_trend = sorted(trends, key=lambda x: x[-1], reverse=True)[0]
        print(trends)
        if max_trend[-1] >= 0.8:
            trends_index = trends.index(max_trend)
            if trends_index <= self.left_level - 1:
                print(f"将左侧的第{trends_index}层移到最右层，之后转置")
            else:
                print(f"将顶部的第{trends_index - self.left_level}层移到最下层")
        else:
            print("No obvious trends!")

    def max_min_imum(self):
        """
        判断cell value是否是一系列值中的最大值/最小值
        :return:
        """
