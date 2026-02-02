import requests
import pandas as pd

JSON_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
OUTPUT_PATH = "/opt/airflow/dags/output/json"

def process_json():
    response = requests.get(JSON_URL)
    data = response.json()

    rows = []

    for pet in data.get("pets", []):
        rows.append({
            "name": pet.get("name"),
            "species": pet.get("species"),
            "birth_year": pet.get("birthYear"),
            "fav_foods": ", ".join(pet.get("favFoods", [])),
            "photo": pet.get("photo")
        })

    df = pd.DataFrame(rows)
    df.to_csv(f"{OUTPUT_PATH}/output.csv", index=False)


