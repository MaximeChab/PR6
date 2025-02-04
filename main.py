from etl.extract import get_disruptions
from etl.transform import process_and_save

if __name__ == "__main__":
    data = get_disruptions()
    if data:
        process_and_save(data)
        print("Data processing completed successfully!")
    else:
        print("Failed to retrieve data.")
