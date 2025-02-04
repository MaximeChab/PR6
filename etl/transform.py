import json
import pandas as pd

def save_json(data, filename):
    """Save data as a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_dataframe(json_data, csv_filename, parquet_filename):
    """Convert JSON data to a DataFrame and save it as CSV and Parquet."""
    if json_data:  # Ensure there is data to save
        df = pd.DataFrame(json_data)
        df.to_csv(csv_filename, index=False, encoding="utf-8")
        df.to_parquet(parquet_filename, index=False, engine="pyarrow")  # Save as Parquet
        print(f"Data saved to {csv_filename} and {parquet_filename}")
    else:
        print(f"No data available for {csv_filename} and {parquet_filename}")

def process_and_save(data):
    """Extract 'disruptions' and 'lines' from JSON, then save them in multiple formats."""
    if data:
        disruptions = data.get("disruptions", [])
        lines = data.get("lines", [])

        # Save disruptions
        save_json(disruptions, "disruptions.json")
        save_dataframe(disruptions, "disruptions.csv", "disruptions.parquet")

        # Save lines
        save_json(lines, "lines.json")
        save_dataframe(lines, "lines.csv", "lines.parquet")

