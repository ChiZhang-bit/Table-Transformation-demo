import pandas as pd


class TableInsight(object):
    def __init__(self, table, table_id):
        self.table = table
        self.table_id = table_id

    def __str__(self):
        return f"The table_id is {self.table_id}"