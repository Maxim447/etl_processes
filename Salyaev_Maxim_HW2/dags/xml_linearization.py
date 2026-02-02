import requests
import pandas as pd
import xml.etree.ElementTree as ET

XML_URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
OUTPUT_PATH = "/opt/airflow/dags/output/xml"

def process_xml():
    response = requests.get(XML_URL)
    root = ET.fromstring(response.text)

    rows = []

    for food in root.findall("food"):
        calories = food.find("calories")
        vitamins = food.find("vitamins")
        minerals = food.find("minerals")

        rows.append({
            "name": food.findtext("name"),
            "mfr": food.findtext("mfr"),
            "serving_value": food.findtext("serving"),
            "serving_units": food.find("serving").attrib.get("units"),
            "calories_total": calories.attrib.get("total") if calories is not None else None,
            "calories_fat": calories.attrib.get("fat") if calories is not None else None,
            "total_fat": food.findtext("total-fat"),
            "saturated_fat": food.findtext("saturated-fat"),
            "cholesterol": food.findtext("cholesterol"),
            "sodium": food.findtext("sodium"),
            "carb": food.findtext("carb"),
            "fiber": food.findtext("fiber"),
            "protein": food.findtext("protein"),
            "vitamin_a": vitamins.findtext("a") if vitamins is not None else None,
            "vitamin_c": vitamins.findtext("c") if vitamins is not None else None,
            "calcium": minerals.findtext("ca") if minerals is not None else None,
            "iron": minerals.findtext("fe") if minerals is not None else None,
        })

    df = pd.DataFrame(rows)
    df.to_csv(f"{OUTPUT_PATH}/output.csv", index=False)