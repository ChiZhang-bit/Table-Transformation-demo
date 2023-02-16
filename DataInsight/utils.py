import pandas as pd
import numpy as np
import scipy.stats as ss


def iqr_rule(t_data: pd.Series, cell_value):
    mean1, mean3 = t_data.quantile(.25), t_data.quantile(.75)
    iqr = mean3 - mean1
    lower, upper = mean1 - 1.5 * iqr, mean3 + 1.5 * iqr
    # print(f"The range is [{lower},{upper}]")
    if cell_value > upper:
        return cell_value - upper
    elif cell_value < lower:
        return lower - cell_value
    else:
        return 0


def three_sigma(t_data: pd.Series, cell_value):
    mu, std = np.mean(t_data), np.std(t_data)
    lower, upper = mu - 3 * std, mu + 3 * std
    # print(f"The range is [{lower},{upper}]")
    if cell_value > upper:
        return cell_value - upper
    elif cell_value < lower:
        return lower - cell_value
    else:
        return 0


def trendline(data: list):
    order = 1  # 使用最小二乘法你和的自由度
    index = [i for i in range(1, len(data) + 1)]
    coeffs = np.polyfit(index, list(data), order)
    slope = coeffs[0]  # 斜率
    fun = np.poly1d(coeffs)
    corelation = np.corrcoef(data, fun(index))
    return slope, corelation[0][1]


def relation(data: np.array):
    return np.corrcoef(data)


def swapPositions(a: list, pos1, pos2):
    a[pos1], a[pos2] = a[pos2], a[pos1]
    return a


class Correlation_Matrix():
    def __init__(self):
        self.data = {}
        self.count = {}

    def add_item(self, key1, key2, num):
        key = (key1, key2)
        if key in self.data.keys():
            ct = self.count[key]
            self.data[key] = (self.data[key] * ct + num) / (ct + 1)
            self.count[key] = ct + 1
        else:
            self.data[key] = num
            self.count[key] = 1

    def find_item(self, key1, key2):
        key = (key1, key2)
        return self.data[key]

    def __str__(self):
        return f"The Correlation Matrix is:\n " \
               f"{self.data}"

    def sort(self, reverse=True):
        self.data = dict(sorted(self.data.items(), key=lambda x: x[1], reverse=reverse))
        return sorted(self.data.items(), key=lambda x: x[1], reverse=reverse)


def calc_max_relation(co_arr: np.array, headers: dict):
    store = Correlation_Matrix()
    for key1 in headers.keys():
        for key2 in headers.keys():
            if headers[key1] != headers[key2]:
                store.add_item(headers[key1], headers[key2], co_arr[key1][key2])
    return store



