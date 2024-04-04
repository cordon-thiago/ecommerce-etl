import pytest
import pandas as pd
import sys
sys.path.insert(0, '../functions')
from functions import etl_functions as etl

@pytest.fixture
def test_data():
    data = {
        "key_column": [1,1,2,3],
        "value_column": ["A", "B", "C", None]
    }
    df = pd.DataFrame(data=data)
    return df

def test_remove_duplicity(test_data):
    df_transformed = etl.remove_duplicity(test_data, "key_column")
    assert len(df_transformed) == 3
    
def test_fill_missing(test_data):
    df_transformed = etl.fill_missing(test_data, {"value_column": "N/A"})
    assert df_transformed.isnull().sum().sum() == 0
