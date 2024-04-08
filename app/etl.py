import pandas as pd
import argparse
from functions import etl_functions as etl, helper_functions as helper

# Define and parse args
def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--param_file",
        type=str,
        required=True,
        help="YML etl parameter file."
    )
    
    return parser.parse_args()

def main():

    try:
        # Log
        log_file = "etl.log"
        logger = helper.create_log_handler(__name__, log_file, "INFO")

        # Get args
        args = parse_args()
    
        # Get config
        etl_params = helper.get_config(args.param_file)
        logger.info("Parameters read successfully.")

        #run ETL
        etl.etl_run(etl_params, logger)
        logger.name = __name__
        logger.info("ETL finished.")
    except Exception:
        logger.exception("Exception occurred.")

if __name__ == "__main__":
    main()