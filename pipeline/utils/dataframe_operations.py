import polars as pl
from typing import Literal


def join_slice(df: pl.DataFrame, df_slice: pl.DataFrame,
               tolerance: str) -> pl.DataFrame:
    if len(df_slice) > 0:
        return df.join_asof(df_slice,
                            on="datetime",
                            strategy="nearest",
                            tolerance=tolerance)
    else:
        return df


def concat_dataframe(
    df1: pl.DataFrame, df2: pl.DataFrame,
    how: Literal['vertical', 'vertical_relaxed', 'diagonal',
                 'diagonal_relaxed', 'horizontal', 'align', 'align_full',
                 'align_inner', 'align_left', 'align_right']
) -> pl.DataFrame:
    return pl.concat([df1, df2], how=how)
