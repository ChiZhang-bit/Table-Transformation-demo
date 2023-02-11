import numpy as np
import pandas as pd

from DataInsight.utils import *


class TableInsight(object):
    def __init__(self, table: pd.DataFrame, table_id: str):
        self.table = table
        self.table_id = table_id
        self.top_header = self.table.keys()
        self.left_header = self.table.index
        self.top_level = len(self.top_header[0])
        self.left_level = len(self.left_header[0])

    # Table Transformation Functions
    def _update(self):
        self.top_header = self.table.keys()
        self.left_header = self.table.index
        try:
            self.top_level = len(self.top_header[0])
        except TypeError:
            self.top_level = 1
        try:
            self.left_level = len(self.left_header[0])
        except TypeError:
            self.left_level = 1
        self.table.index.names = range(self.left_level)
        self.table.columns.names = range(self.top_level)

    def transform_left(self, level_id1: int, level_id2: int):
        if level_id1 >= self.top_level or level_id2 >= self.top_level:
            print("Left_level out of range")
            return
        self.table = self.table.swaplevel(level_id1, level_id2)
        self.table.sort_index(inplace=True)
        self._update()

    def transform_top(self, level_id1: int, level_id2: int):
        if level_id1 >= self.top_level or level_id2 >= self.top_level:
            print("Top_level out of range")
            return
        self.table = self.table.transpose()
        self.table = self.table.swaplevel(level_id1, level_id2)
        self.table.sort_index(inplace=True)
        self.table = self.table.transpose()
        self._update()

    def index_to_column(self, reverse=False):
        """
        这里只默认将index的最底层放到column的最底层， reverse是指把column转换成index
        :return:
        """
        if not reverse:
            if self.left_level == 1:
                print("Refuse to flatten table, left_level==1")
                return
            self.table = self.table.unstack()
        if reverse:
            if self.top_level == 1:
                print("Refuse to flatten table, top_level==1")
            self.table = self.table.stack()
        self._update()

    # Auto Merge Table Blocks Functions
    def merge_transformation(self, rows: list, columns: list):
        """
        更加自由的判断能否用来合并不相关的单元格
        :param rows: 第几行？
        :param columns: 第几列？
        :return:
        """

    # Data Insight Analytic Functions
    def _get_single_cell_value(self, row: int, column: int):
        return self.table.iloc[row, column]

    def _get_block_value(self, rows: list, columns: list) -> pd.DataFrame:
        return self.table.iloc[rows, columns]

    def _single_related_data(self, row: int, column: int):
        left_location = self.left_header[row]
        top_location = self.top_header[column]
        # print(f"left_location: {left_location}")
        # print(f"top_location: {top_location}")
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
        cell_value = self._get_single_cell_value(row, column)

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
            print("No outlier!")
        else:
            outlier_index = outliers.index(max(outliers))
            if outlier_index <= self.left_level - 1:
                print(f"Outlier:将左侧的第{outlier_index}层移到最右层")
            else:
                print(f"Outlier:将顶部的第{outlier_index - self.left_level}层移到最下层")

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
        if max_trend[-1] >= 0.8:
            trends_index = trends.index(max_trend)
            if trends_index <= self.left_level - 1:
                print(f"Trend:将左侧的第{trends_index}层移到最右层，之后转置")
            else:
                print(f"Trend:将顶部的第{trends_index - self.left_level}层移到最下层")
        else:
            print("No obvious trends!")

    def single_max_min_imum(self, row: int, column: int):
        """
        判断cell value是否是一系列值中的最大值/最小值
        :return:
        """
        # Step1: find current cell location
        cell_value = self._get_single_cell_value(row, column)

        # Step2: 根据层次找到与其相关的单元格，先找LEFT，再找TOP，
        # 顺序如下： 左侧表头1-n, 顶侧表头1-m
        related_data = self._single_related_data(row, column)

        # Step3: 根据不同的数据列判断cell value是否是最大值/最小值
        max_min_list = []
        for i, data_item in enumerate(related_data):
            if len(data_item) < 4:
                continue
            temp_list = sorted(data_item)
            if temp_list[0] == cell_value:
                max_min_list.append((i, cell_value - temp_list[1]))
            if temp_list[-1] == cell_value:
                max_min_list.append((i, cell_value - temp_list[-2]))

        # Step4: 选取尽可能大/尽可能小的值来处理
        if len(max_min_list) > 0:
            max_min_list.sort(key=lambda x: abs(x[-1]), reverse=True)
            if max_min_list[0][1] <= 0:
                print(f"是最小值，比第二小少{-max_min_list[0][1]}")
            else:
                print(f"是最大值，比第二大大{max_min_list[0][1]}")
            max_min_index = max_min_list[0][0]
            if max_min_index <= self.left_level - 1:
                print(f"Max&minimum:将左侧的第{max_min_index}层移到最右层，之后转置")
            else:
                print(f"Max&minimum:将顶部的第{max_min_index - self.left_level}层移到最下层")
        else:
            print("Not any max/min!")

    def _block_flat_data(self, block_data: pd.DataFrame):
        """
        发现Block Data内的一块区域，并将其展平，返回一个列表
        :param block_data:
        :return:
        """
        left_related_data = []
        for i in range(self.left_level):
            re_level = list(range(self.left_level))
            re_level.pop(i)
            tmp_data = block_data.reset_index(level=re_level, drop=True)
            if len(set(tmp_data.index)) == 1:  # 如果该层级的表头等于一个就不记录了
                left_related_data.append(None)
                continue
            tmp_data.sort_index(inplace=True)
            left_related_data.append(np.array(tmp_data).flatten())

        top_related_data = []
        tmp_data = block_data.transpose()
        for i in range(self.top_level):
            re_level = list(range(self.top_level))
            re_level.pop(i)
            tmp_data = tmp_data.reset_index(level=re_level, drop=True)
            if len(set(tmp_data.index)) == 1:  # 如果该层级的表头等于一个就不记录了
                top_related_data.append(None)
                continue
            tmp_data.sort_index(inplace=True)
            top_related_data.append(np.array(tmp_data).flatten())

        return left_related_data, top_related_data

    def block_trend(self, rows: list, columns: list):
        # Step1: find current table block
        block_data = self._get_block_value(rows, columns)
        print(block_data, "\n", block_data.index)

        # Step2: 把数据放到一列上
        left_related_data, top_related_data = self._block_flat_data(block_data)

        # Step3: 根据不同的数据列使用趋势评估方法来量化上升/下降的趋势
        trends = []
        for i in left_related_data:
            if i is None:
                trends.append((0, 0))
                continue
            numlist = list(i)
            if len(i) >= 3:
                trends.append(trendline(numlist))
            else:
                trends.append((0, 0))

        for i in top_related_data:
            if i is None:
                trends.append((0, 0))
                continue
            numlist = list(i)
            if len(i) >= 3:
                trends.append(trendline(numlist))
            else:
                trends.append((0, 0))
        print(trends)

        # Step4: 根据相关系数>=0.8 (强相关的判定)，判断数据趋势的变换是否明显
        max_trend = sorted(trends, key=lambda x: x[-1], reverse=True)[0]
        if max_trend[-1] >= 0.8:
            trends_index = trends.index(max_trend)
            if trends_index <= self.left_level - 1:
                print(f"Block Trend:将左侧的第{trends_index}层保留，其余移到上层表头")
            else:
                print(f"Block Trend:将顶部的第{trends_index - self.left_level}层移到左侧，其余所有都移到上层")
        else:
            print("No obvious block trends!")

    def __str__(self):
        return f"The table_id is {self.table_id}\n" \
               f"The table top_header is {self.top_header}, level:{self.top_level}\n" \
               f"The table left_header is {self.left_header}, level:{self.left_level}\n" \
               f"The table is {self.table}"