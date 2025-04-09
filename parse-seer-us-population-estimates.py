import polars as pl

import pathlib
import gzip
import shutil
import urllib.request
import ssl
import certifi
from typing import List, Dict, Tuple, Generator, Optional, Union, Set


def download_seer_us_population_data(url: str, compressed_fpath: pathlib.Path, output_fpath: pathlib.Path) -> None:
    # Create a custom SSL context using certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Download the file
    print(f"Downloading SEER US population data file from {url}...")
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl_context))
        urllib.request.install_opener(opener)
        urllib.request.urlretrieve(url, compressed_fpath)
        print(f"File downloaded to {compressed_fpath}")
    except Exception as e:
        print(f"SSL error encountered: {e}")

    # Decompress the file
    print("Decompressing file...")
    with gzip.open(compressed_fpath, 'rb') as f_in:
        with open(output_fpath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"File decompressed to {output_fpath}")

    # Remove the downloaded, compressed file to save space
    if compressed_fpath.exists():
        compressed_fpath.unlink()
        print(f"Removed compressed file: {compressed_fpath}")
    else:
        print(f"Compressed file not found: {compressed_fpath}")


def parse_line(line: str, year_filter: Optional[Union[int, Set[int]]] = None) -> Dict:
    """
    Parse a single line from the SEER population data file.

    Args:
        line: A line from the SEER population data file
        year_filter: Optional filter for specific year(s). Can be an integer or a set of integers.

    Returns:
        A dictionary with parsed data or None if the line doesn't match the filter
    """
    try:
        # SEER population data record contains two columns, split them
        parts = line.split()
        if len(parts) != 2:
            print(f"Unexpected format: {line}")
            return None

        first_part, second_part = parts

        # Extract fields from the first part
        year = int(first_part[0:4])

        # Skip records that don't match the year filter
        if year_filter is not None:
            if isinstance(year_filter, int) and year != year_filter:
                return None
            elif isinstance(year_filter, set) and year not in year_filter:
                return None

        state = first_part[4:6]
        state_fips = first_part[6:8]
        county_fips = first_part[8:11]

        # Extract fields from the second part
        race = int(second_part[0])
        origin = int(second_part[1])
        sex = int(second_part[2])
        age = int(second_part[3:5])
        population = int(second_part[5:])

        # Determine race description based on year
        if year < 1990:
            race_desc = {1: "White", 2: "Black", 3: "Other"}.get(race, "Unknown")
        else:
            race_desc = {
                1: "White",
                2: "Black",
                3: "American Indian/Alaska Native",
                4: "Asian or Pacific Islander"
            }.get(race, "Unknown")

        # Determine origin description (only for 1990+)
        if year >= 1990:
            origin_desc = {0: "Non-Hispanic", 1: "Hispanic", 9: "Not Applicable"}.get(origin, "Unknown")
        else:
            origin_desc = "Not Applicable"

        # Determine sex description
        sex_desc = {1: "Male", 2: "Female"}.get(sex, "Unknown")

        return {
            "Year": year,
            "State": state,
            "State_FIPS": state_fips,
            "County_FIPS": county_fips,
            "Race_Code": race,
            "Race": race_desc,
            "Origin_Code": origin,
            "Origin": origin_desc,
            "Sex_Code": sex,
            "Sex": sex_desc,
            "Age": age,
            "Population": population
        }
    except Exception as e:
        print(f"Error parsing line: {line}")
        print(f"Error details: {e}")
        return None


def read_in_chunks(file_path: pathlib.Path, year_filter: Optional[Union[int, Set[int]]] = None, chunk_size: int = 100000) -> \
        Generator[List[Dict], None, None]:
    """
    Read the file in chunks to avoid loading everything into memory.

    Args:
        file_path: Path to the demographic data file
        year_filter: Optional filter for specific year(s)
        chunk_size: Number of records to process in each chunk

    Yields:
        Lists of parsed demographic data dictionaries
    """
    with open(file_path, 'r') as file:
        lines = []
        skipped_count = 0
        processed_count = 0

        for line in file:
            line = line.strip()
            if line:
                parsed = parse_line(line, year_filter)
                if parsed:
                    lines.append(parsed)
                    processed_count += 1
                else:
                    skipped_count += 1

            if len(lines) >= chunk_size:
                print(f"Processed: {processed_count}, Skipped: {skipped_count} (filtered out or errors)")
                yield lines
                lines = []

        if lines:  # Don't forget the last chunk
            print(f"Final stats - Processed: {processed_count}, Skipped: {skipped_count} (filtered out or errors)")
            yield lines


def process_seer_population_data(
        file_path: pathlib.Path,
        year_filter: Optional[Union[int, Set[int]]] = None,
        filter_expr: Optional[pl.Expr] = None,
        output_parquet_path: Optional[pathlib.Path] = None,
        output_csv_path: Optional[pathlib.Path] = None,
        chunk_size: int = 100_000
) -> pl.DataFrame:
    """
    Process the demographic data file and create a Polars DataFrame with optional filtering.

    Args:
        file_path: Path to the demographic data file
        year_filter: Year(s) to filter for. Can be a single year (int) or multiple years (set of ints)
        filter_expr: A Polars expression for filtering the data (if None, no filtering is applied)
        output_parquet_path: Output directory for parquet files (if None, no parquet is saved)
        output_csv_path: Path for output CSV file (if None, no CSV is saved)
        chunk_size: Number of records to process in each chunk

    Returns:
        A Polars DataFrame containing the processed (and optionally filtered) data
    """
    if output_parquet_path:
        output_parquet_path = pathlib.Path(output_parquet_path)
        if not output_parquet_path.exists():
            output_parquet_path.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {output_parquet_path}")

    # Define schema to optimize memory usage
    schema = {
        "Year": pl.Int16,
        "State": pl.Categorical,
        "State_FIPS": pl.Categorical,
        "County_FIPS": pl.Categorical,
        "Race_Code": pl.UInt8,
        "Race": pl.Categorical,
        "Origin_Code": pl.UInt8,
        "Origin": pl.Categorical,
        "Sex_Code": pl.UInt8,
        "Sex": pl.Categorical,
        "Age": pl.UInt8,
        "Population": pl.Int32
    }

    # Determine what to print based on year filter
    if year_filter is None:
        year_desc = "all years"
    elif isinstance(year_filter, int):
        year_desc = f"year {year_filter}"
    else:
        year_desc = f"years {', '.join(map(str, year_filter))}"

    print(f"Starting to process data ({year_desc})...")

    # Process the file in chunks
    dfs = []
    for i, chunk in enumerate(read_in_chunks(file_path, year_filter, chunk_size)):
        print(f"Processing chunk {i + 1}...")
        if not chunk:  # Skip empty chunks
            continue

        chunk_df = pl.DataFrame(chunk).with_columns([
            pl.col(col).cast(dtype) for col, dtype in schema.items()
        ])

        # Apply additional filter if provided
        if filter_expr is not None:
            chunk_df = chunk_df.filter(filter_expr)

        if chunk_df.height == 0:
            print(f"No matching records in chunk {i + 1} after filtering")
            continue

        # Either save to disk or keep in memory depending on size
        if output_parquet_path:
            chunk_path = output_parquet_path / f"filtered_chunk_{i}.parquet"
            chunk_df.write_parquet(chunk_path)
            print(f"Saved chunk {i + 1} to {chunk_path}")
            dfs.append(str(chunk_path))  # Convert Path to string here
        else:
            dfs.append(chunk_df)

    # Combine all chunks
    if not dfs:
        print("No matching records found!")
        return pl.DataFrame(schema=schema)

    if output_parquet_path and all(isinstance(df, str) for df in dfs):
        # If we saved chunks to disk, read and combine them
        final_df = pl.concat([pl.read_parquet(path) for path in dfs])

        # Clean up intermediate files
        for path_str in dfs:
            path = pathlib.Path(path_str)
            if path.exists():
                path.unlink()
    else:
        # If we kept chunks in memory
        final_df = pl.concat(dfs)

    # Save as CSV if specified
    if output_csv_path and final_df.height > 0:
        final_df.write_csv(output_csv_path)
        print(f"Saved filtered data to: {str(output_csv_path)}")

    return final_df


def print_summary(df: pl.DataFrame, description: str = "Filtered Dataset"):
    """Print a summary of the DataFrame."""
    print(f"\n{description} Summary:")
    if df.height == 0:
        print("No matching records found.")
        return

    print(f"Total matching records: {df.height}")
    print(f"Memory usage: {df.estimated_size() / (1024 ** 2):.2f} MB")

    # Display a sample of the data
    print("\nSample data:")
    print(df.head(5))

    # Basic analysis
    print("\nPopulation by year:")
    year_summary = df.group_by("Year").agg(pl.sum("Population").alias("Total_Population")).sort("Year")
    print(year_summary)

    print("\nPopulation distribution by age:")
    age_summary = df.group_by(["Year", "Age"]).agg(pl.sum("Population").alias("Total_Population")).sort(["Year", "Age"])
    print(age_summary.head(15))  # Show first 15 rows

    print("\nPopulation by state:")
    state_summary = df.group_by("State").agg(pl.sum("Population").alias("Total_Population")).sort("Total_Population",
                                                                                                  descending=True)
    print(state_summary.head(10))  # Show top 10 states


if __name__ == "__main__":
    # Set up output directory
    data_dir = "data"
    data_dir = pathlib.Path(data_dir)
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {data_dir}")

    compressed_file_path = data_dir / "seer_us_population_data.txt.gz"
    output_file_path = data_dir / "seer_us_population_data.txt"
    filtered_file_path = data_dir / "seer_2011_white_nonhispanic_female_50to75.csv"

    # SEER US Population Data URL
    url_seer = "https://seer.cancer.gov/popdata/yr1990_2023.singleages.through89.90plus/us.1990_2023.singleages.through89.90plus.adjusted.txt.gz"
    download_seer_us_population_data(url_seer, compressed_file_path, output_file_path)

    # Filter: Single year (2011) - White, Non-Hispanic, Female, Ages 50-75
    year_filter = 2011
    filter_expr = (
            (pl.col("Race_Code") == 1) &    # White
            (pl.col("Origin_Code") == 0) &  # Non-Hispanic
            (pl.col("Sex_Code") == 2) &     # Female
            (pl.col("Age") >= 50) &         # Age between 50 and 75
            (pl.col("Age") <= 75)
    )

    df = process_seer_population_data(
        file_path=output_file_path,
        year_filter=year_filter,
        filter_expr=filter_expr,
        output_parquet_path=data_dir / "parquet",
        output_csv_path=filtered_file_path
    )
    print_summary(df)
