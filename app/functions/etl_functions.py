import pandas as pd

def remove_duplicity(df, key_cols):
    """
    Remove duplicity based in a subset of columns.
    """
    return df.drop_duplicates(key_cols)

def cast_columns(df, cast_type, columns):
    """
    Cast columns to a pandas datatype.
    """
    for col in columns:
        df[col] = df[col].astype(cast_type)
    
    return df

def fill_missing(df, values):
    """
    Fill missing values.
    """
    return df.fillna(value=values)

def apply_transformations(df, etl_params):
    """
    Apply transformations to datasets.
    """

    #Remove duplicates
    if etl_params.get("key_columns"):
        df_unique = remove_duplicity(df, etl_params.get("key_columns"))
    else:
        df_unique = df

    #Fill missing
    if etl_params.get("fill_missing"):
        df_fill_missing = fill_missing(df_unique, etl_params.get("fill_missing"))
    else:
        df_fill_missing = df_unique
    
    #Type cast
    if etl_params.get("type_cast"):
        for cast_type, cols in etl_params.get("type_cast").items():
            df_cast = cast_columns(df_fill_missing, cast_type, cols)
    else:
        df_cast = df_fill_missing

    return df_cast