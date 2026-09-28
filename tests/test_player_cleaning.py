import pytest
from pyspark.sql import functions as F
from src.etl.player_cleaning import clean_player_name, clean_player_name_native

TEST_NAME_CASES = [
    ("Vana, Jake", "Jake Vana"),
    ("Palumbo, Chad", "Chad Palumbo"),
    ("Christopher Iuliano", "Christopher Iuliano"),
    ("McMeekin, Andrew", "Andrew McMeekin"),
    ("Croddick, Ryan", "Ryan Croddick"),
    ("Smith, Jr., John", "John Smith Jr."),
    ("Smith, John, Jr.", "John Smith Jr."),
    ("Smith Jr., John", "John Smith Jr."),
    ("O'Neill, Brennan", "Brennan O'Neill"),
    ("Van Horn, David", "David Van Horn"),
    ("Williams, III, Marcus", "Marcus Williams III"),
    ("Davis, IV, Thomas", "Thomas Davis IV"),
    ("TEAM", "TEAM"),
    ("TM", "TM"),
    ("", None),
    (None, None),
]



@pytest.mark.parametrize("raw_name, expected", TEST_NAME_CASES)
def test_clean_player_name(raw_name, expected):
    assert clean_player_name(raw_name) == expected



def test_clean_player_name_native(spark):
    df = spark.createDataFrame([(r, e) for r, e in TEST_NAME_CASES], ["raw_name", "expected"])
    res = df.withColumn("actual", clean_player_name_native(F.col("raw_name"))).collect()
    for row in res:
        assert row["actual"] == row["expected"], f"Failed for {row['raw_name']}: got {row['actual']}"
