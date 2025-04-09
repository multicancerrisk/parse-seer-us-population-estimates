import pathlib
import gzip
import shutil
import urllib.request
import ssl
import certifi


def download_seer_us_population_data(url: str):
    # Set up output directory
    data_dir = "data"
    data_dir = pathlib.Path(data_dir)
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {data_dir}")

    compressed_file_path = data_dir / "seer_us_population_data.txt.gz"
    output_file_path = data_dir / "seer_us_population_data.txt"

    # Create a custom SSL context using certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Download the file
    print(f"Downloading SEER US population data file from {url}...")
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl_context))
        urllib.request.install_opener(opener)
        urllib.request.urlretrieve(url, compressed_file_path)
        print(f"File downloaded to {compressed_file_path}")
    except Exception as e:
        print(f"SSL error encountered: {e}")

    # Decompress the file
    print("Decompressing file...")
    with gzip.open(compressed_file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"File decompressed to {output_file_path}")

    # Remove the downloaded, compressed file to save space
    if compressed_file_path.exists():
        compressed_file_path.unlink()
        print(f"Removed compressed file: {compressed_file_path}")
    else:
        print(f"Compressed file not found: {compressed_file_path}")


if __name__ == "__main__":
    # SEER US Population Data URL
    url_seer = "https://seer.cancer.gov/popdata/yr1990_2023.singleages.through89.90plus/us.1990_2023.singleages.through89.90plus.adjusted.txt.gz"
    download_seer_us_population_data(url_seer)
    print("Process completed successfully!")