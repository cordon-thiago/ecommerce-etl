import pandas as pd
from functions import helper_functions as helper

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

def etl_run(etl_params, logger):
    """
    Run transformation logic.
    """

    # log
    logger.name = __name__

    try:
        #load and transform product dataset
        dataset_name = "product"
        product = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
        logger.info(f"Dataset [{dataset_name}] loaded - # rows read: {len(product)}")
        product_transform = apply_transformations(product, etl_params.get(dataset_name))
        logger.info(f"Dataset [{dataset_name}] transformed - # rows transformed: {len(product_transform)}")
    
        #load and transform order_items dataset
        dataset_name = "order_items"
        order_items = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
        logger.info(f"Dataset [{dataset_name}] loaded - # rows read: {len(order_items)}")
        order_items_transform = apply_transformations(order_items, etl_params.get(dataset_name))
        logger.info(f"Dataset [{dataset_name}] transformed - # rows transformed: {len(order_items_transform)}")

        #load DF and transform order dataset
        dataset_name = "order"
        order = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
        logger.info(f"Dataset [{dataset_name}] loaded - # rows read: {len(order)}")
        order_transform = apply_transformations(order, etl_params.get(dataset_name))
        logger.info(f"Dataset [{dataset_name}] transformed - # rows transformed: {len(order_transform)}")

        # join datasets: order_items + order + product
        join_df = order_items_transform.merge(order_transform, on="order_id", how="left").merge(product_transform, on="product_id", how="left")
        logger.info(f"Datasets joined - # rows: {len(join_df)}")

        # Write to parquet
        output_path = etl_params.get("output").get("output_path")
        partition_columns = etl_params.get("output").get("partition_columns")
        join_df.to_parquet(output_path, partition_cols=partition_columns, existing_data_behavior="delete_matching")
        logger.info(f"Dataset written to: {output_path}")
    except Exception as e:
        logger.exception("Exception occurred when running ETL.")
