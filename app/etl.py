import pandas as pd
import argparse
from functions import etl_functions as etl, helper_functions as helper

# Define and parse args
parser = argparse.ArgumentParser()

parser.add_argument(
    "--param_file",
    type=str,
    required=True,
    help="YML etl parameter file."
)

args = parser.parse_args()

# Get config
etl_params = helper.get_config(args.param_file)

#load and transform product dataset
dataset_name = "product"
product = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
product_transform = etl.apply_transformations(product, etl_params.get(dataset_name))

#load and transform order_items dataset
dataset_name = "order_items"
order_items = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
order_items_transform = etl.apply_transformations(order_items, etl_params.get(dataset_name))

#load DF and transform order dataset
dataset_name = "order"
order = pd.read_csv(etl_params.get(dataset_name).get("source_file"), **etl_params.get(dataset_name).get("source_file_params"))
order_transform = etl.apply_transformations(order, etl_params.get(dataset_name))

# join datasets: order_items + order + product
join_df = order_items_transform.merge(order_transform, on="order_id", how="left").merge(product_transform, on="product_id", how="left")

# Write to parquet
output_path = etl_params.get("output").get("output_path")
partition_columns = etl_params.get("output").get("partition_columns")
join_df.to_parquet(output_path, partition_cols=partition_columns, existing_data_behavior="overwrite_or_ignore")
