import pandas as pd
import numpy as np
from functools import cmp_to_key


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



