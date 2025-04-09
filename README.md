# Parse SEER US population estimates
A Python-based utility to download, parse, and filter US population estimates from SEER. The script ingests the 1990-2023 county-level US population data from SEER, containing information at a single-year age level and is further stratified by 4 expanded races by origin (https://seer.cancer.gov/popdata/download.html). The data dictionary can be found here: https://seer.cancer.gov/popdata/popdic.html

The script `parse-seer-us-population-estimates.py` is designed to download the SEER population estimates, parse the data, and filter it based on a specific criteria. The script uses [Polars](https://pola.rs/) for data manipulation. The script can take in a [Polars expression](https://docs.pola.rs/user-guide/expressions/) for data filteration. See below for some examples on the different types of filtration that can be done with the script using Polars expressions.

## Example filters
Please consult the [SEER data dictionary](https://seer.cancer.gov/popdata/popdic.html) to understand the different filtering conditions that are possible. Then craft a [Polars expression](https://docs.pola.rs/user-guide/expressions/) to specify the condition.

### Example 1: Single year (2011) - White, Non-Hispanic, Female, Ages 50-75
By default, this is the filtration that is currently implemented in the script.
```python
# Filter: Single year (2011) - White, Non-Hispanic, Female, Ages 50-75
year_filter = 2011
filter_expr = (
        (pl.col("Race_Code") == 1) &    # White
        (pl.col("Origin_Code") == 0) &  # Non-Hispanic
        (pl.col("Sex_Code") == 2) &     # Female
        (pl.col("Age") >= 50) &         # Age between 50 and 75
        (pl.col("Age") <= 75)
)
```

### Example 2: Multiple years (2000, 2005, 2010) - Asian/Pacific Islander, any Origin, any Sex, Ages 18-30
```python
year_filter = {2000, 2005, 2010}  # Set of years
filter_expr = (
    (pl.col("Race_Code") == 4) &  # Asian/Pacific Islander
    (pl.col("Age") >= 18) &       # Age between 18 and 30
    (pl.col("Age") <= 30)
)
```

### Example 3: All available years - Black males, age 65
```python
year_filter = None
filter_expr = (
    (pl.col("Race_Code") == 2) &  # Black
    (pl.col("Sex_Code") == 1) &   # Male
    (pl.col("Age") == 65)         # Age 65
)
```