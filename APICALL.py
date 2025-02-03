import requests
import json


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




def save_to_file(data, filename="disruptions.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    disruptions = get_disruptions()
    if disruptions:
        save_to_file(disruptions)
        print(f"Results saved to disruptions.json")




