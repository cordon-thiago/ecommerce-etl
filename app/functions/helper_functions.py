import yaml
import logging

def get_config(yml_file, dataset_name=None):
    """
    Reads yml file and returns dictionary with file content.

    Input:
        - yml_file: yml file path.

    Output:
        - Dictionary with yml file content.
    """
    with open(yml_file) as file:
        etl_params = yaml.safe_load(file)

    return etl_params[dataset_name] if dataset_name else etl_params

def create_log_handler(job_name, log_file, level):
    """
    Creates a log handler.
    """

    logger = logging.getLogger(job_name)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s - %(name)s - [%(levelname)s] - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger
