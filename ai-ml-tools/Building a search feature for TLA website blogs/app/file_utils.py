from typing import List

import pandas as pd


def get_title_and_description_from_cols(columns: List[str]):
    title_column = "title"
    description_column = 'description'
    for col in columns:
        if 'title' in col.lower():
            title_column = col
        elif 'description' in col.lower():
            description_column = col

    return title_column, description_column


def get_text_columns(df:pd.DataFrame):
    text_cols = []
    for col in df.columns:
        col_type = df[col].dtype.name
        if col_type == 'o' or any( col_option in col_type for col_option in ("str","text",'object')):
            text_cols.append(col)

    return text_cols
        # for value in
    # title_column = "title"
    # description_column = 'description'
    # for col in columns:
    #     if 'title' in col.lower():
    #         title_column = col
    #     elif 'description' in col.lower():
    #         description_column = col

    return title_column, description_column


class FileSaved:

    def __init__(self):
        self.data = {}

    def file_cached(self, key, value):
        self.data[key] = value
