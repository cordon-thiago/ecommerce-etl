import yaml

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