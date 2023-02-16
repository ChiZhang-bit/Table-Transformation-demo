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
        self._extract_header_list()
        self._extract_sort_dict()

    def _extract_header_list(self):
        """
        提取出表格层次列表，分为 top_header_list 和 left_header_list 两个部分
        :return:
        """
        top_tmp_set = list(map(list, zip(*self.top_header.tolist())))
        self.top_lists = [sorted(list(set(i)), key=i.index) for i in top_tmp_set]
        left_tmp_set = list(map(list, zip(*self.left_header.tolist())))
        self.left_lists = [sorted(list(set(i)), key=i.index) for i in left_tmp_set]
        return

    def _extract_sort_dict(self):
        '''
        根据表格的层次索引结构，提取出去重后的表头结构，形成list
        :return:
        '''
        self._extract_header_list()
        num = 1
        self.sort_dict = {}
        for left_list in self.left_lists:
            for i in left_list:
                self.sort_dict[i] = num
                num += 1
        for top_list in self.top_lists:
            for i in top_list:
                self.sort_dict[i] = num
                num += 1
        return

    def _sort_index_func(self, x: pd.Index):
        x = [self.sort_dict[i] for i in x]
        return pd.Index(x)

    # data_location functions
    def normal_loc_func(self, left_loc: list, top_loc: list):
        for i, item in enumerate(left_loc):
            if item != "*":
                if len(item) == len(self.left_lists[i]):
                    left_loc[i] = "*"

        for i, item in enumerate(top_loc):
            if item != "*":
                if len(item) == len(self.top_lists[i]):
                    top_loc[i] = "*"

        return left_loc, top_loc

    def data_location(self, left_loc: list, top_loc: list):
        """
        left_loc = [*, * , name] 筛选出数据区域
        :param left_loc:
        :param top_loc:
        :return:
        """
        left_loc, top_loc = self.normal_loc_func(left_loc, top_loc)
        data = self.table
        for i in range(len(left_loc)):
            if left_loc[i] == '*':
                continue
            try:
                concat_list = []
                for loci in left_loc[i]:
                    concat_list.append(data.xs(key=loci, axis=0, level=i, drop_level=False))
                data = pd.concat(concat_list, axis=0)
            except KeyError:
                print(f"The key: {loci} is error")
                raise KeyError

        for i in range(len(top_loc)):
            if top_loc[i] == "*":
                continue
            try:
                concat_list = []
                for loci in top_loc[i]:
                    concat_list.append(data.xs(key=loci, axis=1, level=i, drop_level=False))
                data = pd.concat(concat_list, axis=1)
            except KeyError:
                # 这里变换可能会有问题
                print(f"The key:{loci} is error")
                raise KeyError
        data.sort_index(inplace=True, axis=0, key=lambda x: self._sort_index_func(x))
        data.sort_index(inplace=True, axis=1, key=lambda x: self._sort_index_func(x))
        return data

    def data_index_location(self, rows: list, columns: list):
        block_data = self._get_block_value(rows, columns)
        return block_data

    def find_index_num_by_loc_list(self, left_loc: list, top_loc: list):
        """
        根据loc_list 查找 rows 和 columns
        :param left_loc:
        :param top_loc:
        :return: 返回对应的num_list
        """
        top_tmp_set = list(map(list, zip(*self.top_header.tolist())))
        left_tmp_set = list(map(list, zip(*self.left_header.tolist())))
        rows = set(range(self.table.shape[0]))
        columns = set(range(self.table.shape[1]))
        index_dict = {}
        for top_tmp_list in top_tmp_set:
            for index, item in enumerate(top_tmp_list):
                if item in index_dict.keys():
                    index_dict[item].add(index)
                else:
                    index_dict[item] = {index}
        for left_tmp_list in left_tmp_set:
            for index, item in enumerate(left_tmp_list):
                if item in index_dict.keys():
                    index_dict[item].add(index)
                else:
                    index_dict[item] = {index}
        ">>> index_dict: {2018: [0, 1, 2, 3], 'Spring': [0, 4, 8, 12, 16, 20], ...}"
        for left_headers in left_loc:
            tmp = set()
            if left_headers == "*":
                continue
            for left_header in left_headers:
                tmp = tmp | index_dict[left_header]
            rows = rows & tmp
        for top_headers in top_loc:
            tmp = set()
            if top_headers == "*":
                continue
            for top_header in top_headers:
                tmp = tmp | index_dict[top_header]
            columns = columns & tmp
        rows = sorted(list(rows))
        columns = sorted(list(columns))
        return rows, columns

    def find_list_by_index_num(self, rows: list, columns: list):
        """
        根据 rows columns, 返回对应格式要求的index_num
        :param rows:
        :param columns:
        :return:
        """
        top_index = self.top_header[columns].tolist()
        left_index = self.left_header[rows].tolist()

        top_tmp_set = list(map(list, zip(*top_index)))
        top_lists = [sorted(list(set(i)), key=i.index) for i in top_tmp_set]
        left_tmp_set = list(map(list, zip(*left_index)))
        left_lists = [sorted(list(set(i)), key=i.index) for i in left_tmp_set]

        return left_lists, top_lists

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
        self._extract_header_list()
        return

    def transform_left(self, level_id1: int, level_id2: int):
        if level_id1 >= self.left_level or level_id2 >= self.left_level:
            print("Left_level out of range")
            return
        if level_id1 == level_id2:
            print("Same level id in transformation left")
            return
        self.table = self.table.swaplevel(level_id1, level_id2, axis=0)
        self.table.sort_index(inplace=True, axis=0, key=lambda x: self._sort_index_func(x))
        self._update()

    def transform_top(self, level_id1: int, level_id2: int):
        if level_id1 >= self.top_level or level_id2 >= self.top_level:
            print("Top_level out of range")
            return
        if level_id1 == level_id2:
            print("Same level id in transformation top")
            return
        self.table = self.table.swaplevel(level_id1, level_id2, axis=1)
        self.table.sort_index(inplace=True, axis=1, key=lambda x: self._sort_index_func(x))
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
    def merge_transformation_by_headers(self, left_loc: list, top_loc: list):
        """
        根据已有的headers，来进行合并，经过分析规则， location中的*需要尽可能的靠后，
        (*，1) -> (1,*)
        :param left_loc:
        :param top_loc:
        :return:
        """
        left_loc, top_loc = self.normal_loc_func(left_loc, top_loc)
        loc1_queue = []
        transform_result = []
        for i in range(len(left_loc)):
            if left_loc[i] == "*":
                loc1_queue.insert(0, i)
            elif len(loc1_queue) > 0:
                i_loc = loc1_queue.pop()
                self.transform_left(i_loc, i)  # 将loc1_queue 与 i 交换位置
                swapPositions(left_loc, i_loc, i)

        loc1_queue.clear()
        transform_result.clear()
        for i in range(len(top_loc)):
            if top_loc[i] == "*":
                loc1_queue.insert(0, i)
            elif len(loc1_queue) > 0:
                i_loc = loc1_queue.pop()
                self.transform_top(i_loc, i)
                swapPositions(top_loc, i_loc, i)
        self._update()
        return left_loc, top_loc

    # Data Insight Analytic Functions
    def _get_single_cell_value(self, row: int, column: int):
        return self.table.iloc[row, column]

    def _get_block_value(self, rows: list, columns: list) -> pd.DataFrame:
        return self.table.iloc[rows, columns]

    def _single_related_data(self, row: int, column: int):
        left_location = self.left_header[row]
        top_location = self.top_header[column]
        related_data = []
        for i in range(self.left_level):
            data = self.table.xs(top_location, axis=1, drop_level=True)
            for j in range(self.left_level):
                if i != j:
                    data = data.xs(left_location[j], level=j, axis=0, drop_level=False)
            related_data.append(data)

        for i in range(self.top_level):
            data = self.table.xs(left_location, axis=0, drop_level=True)
            for j in range(self.top_level):
                if i != j:
                    data = data.xs(top_location[j], level=j, axis=0, drop_level=False)
            related_data.append(data)
        return related_data

    def single_outlier(self, rows: list, columns: list):
        """
        假定只有一个单元格， 评判一个single_cell的outlier, 评估其异常程度
        :param: rows:
        :param: columns:
        :return: 异常程度
        """
        if len(rows) != 1 or len(columns) != 1:
            print("Not single cell!")
            return
        else:
            row, column = rows[0], columns[0]
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

    def single_trend(self, rows: list, columns: list):
        """
        假定只有一个单元格，评判一个single_cell的outlier, 评估其异常程度
        :param columns:
        :param rows:
        :return:
        """
        if len(rows) != 1 or len(columns) != 1:
            print("Not single cell!")
            return
        else:
            row, column = rows[0], columns[0]
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

    def single_max_min_imum(self, rows: list, columns: list):
        """
        判断cell value是否是一系列值中的最大值/最小值
        :param: rows:
        :param: columns:
        :return:
        """
        if len(rows) != 1 or len(columns) != 1:
            print("Not single cell!")
            return
        else:
            row, column = rows[0], columns[0]

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
        :param block_data: 原始的DataFrame
        :return:
        """
        left_related_data = []
        for i in range(self.left_level):
            # 按照各个等级，只保留该等级的表头
            re_level = list(range(self.left_level))
            re_level.pop(i)
            tmp_data = block_data.reset_index(level=re_level, drop=True)
            if len(set(tmp_data.index)) == 1:  # 如果该层级的表头等于一个就不记录了
                left_related_data.append(None)
                continue
            tmp_data.sort_index(inplace=True, key=lambda x: self._sort_index_func(x))
            left_related_data.append(np.array(tmp_data).flatten())

        top_related_data = []
        tmp_data1 = block_data.transpose()
        for i in range(self.top_level):
            # 按照各个等级，只保留该等级的表头
            re_level = list(range(self.top_level))
            re_level.pop(i)
            tmp_data = tmp_data1.reset_index(level=re_level, drop=True)
            if len(set(tmp_data.index)) == 1:  # 如果该层级的表头等于一个就不记录了
                top_related_data.append(None)
                continue
            tmp_data.sort_index(inplace=True, key=lambda x: self._sort_index_func(x))
            top_related_data.append(np.array(tmp_data).flatten())

        return left_related_data, top_related_data

    def block_trend(self, left_loc: list, top_loc: list):
        '''
        已知Block的情况下，根据Block展平的方式，判断Block是否内含有趋势
        :param left_loc:
        :param top_loc:
        :return:
        '''
        # Step1: 合并，之后 find current table block
        left_loc, top_loc = self.merge_transformation_by_headers(left_loc=left_loc, top_loc=top_loc)
        # 合并之后需要根据变换规则把left_loc和top_loc进行调整
        block_data = self.data_location(left_loc, top_loc)

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

        # Step4: 根据相关系数>=0.75 (强相关的判定)，判断数据趋势的变换是否明显
        max_trend = sorted(trends, key=lambda x: x[-1], reverse=True)[0]
        if max_trend[-1] >= 0.8:
            trends_index = trends.index(max_trend)
            if trends_index <= self.left_level - 1:
                print(f"Block Trend:将左侧的第{trends_index}层保留，其余移到上层表头")
            else:
                print(f"Block Trend:将顶部的第{trends_index - self.left_level}层移到左侧，其余所有都移到上层")
        else:
            print("No obvious block trends!")

    def block_correlation(self, left_loc: list, top_loc: list):
        """
        查找Block内有无correlation的关系, correlation的关系主要在于两个列，
        correlation的列应该放在左侧或者上侧的最底层
        :return: 对表格进行适当的变换
        """
        # Step1: 合并，之后将对应的数据块筛选出来
        left_loc, top_loc = self.merge_transformation_by_headers(left_loc=left_loc, top_loc=top_loc)
        # 合并之后需要根据变换规则把left_loc和top_loc进行调整
        block_data = self.data_location(left_loc, top_loc)

        # Step2: 穷举所有可能的具有关系的列
        left_related_data = []
        for i in range(self.left_level):
            reverse_level = list(range(self.left_level))
            reverse_level.remove(i)
            data = block_data.reset_index(reverse_level, drop=True)
            data.sort_index(key=lambda x: self._sort_index_func(x), inplace=True)
            left_related_data.append(data)

        trans_block_data = block_data.transpose()
        top_related_data = []
        for i in range(self.top_level):
            reverse_level = list(range(self.top_level))
            reverse_level.remove(i)
            data = trans_block_data.reset_index(reverse_level, drop=True)
            data.sort_index(key=lambda x: self._sort_index_func(x), inplace=True)
            top_related_data.append(data)

        # Step3: 针对数据关系列，找出他们之间的皮尔逊相关系数
        co_list = []
        for i, item in enumerate(left_related_data):
            arr = np.array(item)
            row_names = {i: name for i, name in enumerate(list(item.index))}
            co_arr = relation(arr)
            # 求相关性,平均值求最大：
            store = calc_max_relation(co_arr, row_names).sort()
            if len(store) > 0:
                co_list.append((i, store[0]))

        for i, item in enumerate(top_related_data):
            arr = np.array(item)
            row_names = {i: name for i, name in enumerate(list(item.index))}
            co_arr = relation(arr)
            # 求相关性,平均值求最大：
            store = calc_max_relation(co_arr, row_names).sort()
            if len(store) > 0:
                co_list.append((i+self.left_level, store[0]))

        # Step4: 根据所有得到的最优Correlation的值进行排序，得到最具有Correlation的一部分
        co_list.sort(key=lambda x: x[1][1], reverse=True)
        print(co_list)
        indexs = co_list[0][1][0]
        relation_result = co_list[0][1][1]
        loc = co_list[0][0]
        print(f"最具有关系的是：{indexs}, 他们的相关性是:{relation_result}")
        if loc >= self.left_level:
            print(f"位于上层的第{loc - self.left_level}层，将他放置到最下层（最底层）")
            self.transform_top(loc - self.left_level, self.top_level-1)
        else:
            print(f"位于左侧的第{loc}层，将他放置到最右层（最底层）")
            self.transform_left(loc, self.left_level-1)
        print(self.table)
        return

    def explortory_tree(self, initial_left_loc, initial_top_loc, i=0):
        """
        以[*,*,*,*]替换其中的部分用于探索式的分析层次表个数据
        :param initial_left_loc:
        :param initial_top_loc:
        :param i: 第几层
        :return:
        """
        print("------------------------Level {}---------------------------".format(i))
        print(self.data_location(initial_left_loc, initial_top_loc))

        for index in range(len(initial_left_loc)):
            if initial_left_loc[index] != "*":
                tmp = initial_left_loc[index]
                initial_left_loc[index] = "*"
                self.explortory_tree(initial_left_loc, initial_top_loc, i=i + 1)
                initial_left_loc[index] = tmp

        for index in range(len(initial_top_loc)):
            if initial_top_loc != "*":
                tmp = initial_top_loc[index]
                initial_top_loc[index] = "*"
                self.explortory_tree(initial_left_loc, initial_top_loc, i=i + 1)
                initial_top_loc[index] = tmp

    def decision_transformation_way(self, left_loc, top_loc, rows, columns):
        pass

    def __str__(self):
        return f"The table_id is {self.table_id}\n" \
               f"The table top_header is {self.top_header}, level:{self.top_level}\n" \
               f"The table left_header is {self.left_header}, level:{self.left_level}\n" \
               f"The table is {self.table}"
