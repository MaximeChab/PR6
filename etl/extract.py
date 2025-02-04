import requests
import json
import pandas as pd  # Import pandas for CSV conversion

def get_disruptions():
    url = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"
    headers = {"apiKey": "zu1X4DO1FAyzGs9DPp96Aq79vjiRC9He"}
   
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
   
    return None


def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def json_to_csv(json_data, csv_filename):
    if json_data:  # Ensure there is data to save
        df = pd.DataFrame(json_data)
        df.to_csv(csv_filename, index=False, encoding="utf-8")
    else:
        print(f"No data available for {csv_filename}")


if __name__ == "__main__":
    data = get_disruptions()
    if data:
        # Extract disruptions and lines separately
        disruptions = data.get("disruptions", [])
        lines = data.get("lines", [])

        # Save disruptions as JSON and CSV
        save_json(disruptions, "disruptions.json")
        json_to_csv(disruptions, "disruptions.csv")
        print("Disruptions saved to disruptions.json and disruptions.csv")

        # Save lines as JSON and CSV
        save_json(lines, "lines.json")
        json_to_csv(lines, "lines.csv")
        print("Lines saved to lines.json and lines.csv")
