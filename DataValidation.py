import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()
GoogleAPI = os.getenv('GoogleAPI')

compare_file = "nfcollectiveCompare.xlsx"    # Input file
output_file = "nfcollectiveCompareV14.xlsx"   # Output file

def get_clinic_data(clinic_name):
    search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"

    search_params = {'query': clinic_name, 'key': GoogleAPI}

    search_res = requests.get(search_url, params=search_params).json()
    if search_res.get("status") != "OK" or not search_res.get("results"):
        return None

    place_id = search_res["results"][0]["place_id"]

    details_params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,types",
        "key": GoogleAPI,
    }
    details_res = requests.get(details_url, params=details_params).json()
    if details_res.get("status") != "OK":
        return None

    r = details_res["result"]
    return {
        "name": r.get("name", ""),
        "address": r.get("formatted_address", ""),
        "phone": r.get("formatted_phone_number", ""),
        "website": r.get("website", ""),
        "specialty": r.get("types", []),

    }
# Testing fnction
# print("Data from this clinic : ",get_clinic_data("David H. Gutmann, MD, PhD"))

def extract_address_parts(address):
    if not address:
        return "", "", "", "", ""
    parts = [p.strip() for p in address.split(",")]
    street, city, state, zip_code, country = "", "", "", "", ""
    if len(parts) >= 2:
        street, city = parts[0], parts[1]
    if len(parts) >= 3:
        tokens = parts[2].split()
        state = tokens[0]
        zip_code = " ".join(tokens[1:]) if len(tokens) > 1 else ""
    if len(parts) >= 4:
        country = parts[-1]
    return street, city, state, zip_code, country

# ---------------- Load & Update ----------------
df = pd.read_excel(compare_file)  

for idx, row in df.iterrows():
    clinic_name = row.get("physicians_1_ORIGINAL")
    if not clinic_name:
        clinic_name = row.get("hospital", "")

        continue

    print(f"Processing {clinic_name}...")
    clinic_info = get_clinic_data(str(clinic_name))
    if not clinic_info:
        clinic_info = get_clinic_data(str(row.get('hospital')) + " hospital")
        df.at[idx, "hospital_NEW"] = clinic_info["name"]

        print(f"Clinic not found: hospital is:  {clinic_info}")
        print("Not found")
        continue

    # Update NEW columns
    df.at[idx, "physicians_1_NEW"] = clinic_info["name"]
    df.at[idx, "phone_NEW"] = clinic_info["phone"]

    street, city, state, zip_code, country = extract_address_parts(clinic_info["address"])
    df.at[idx, "street_address_NEW"] = street
    df.at[idx, "provider_city_NEW"] = city
    df.at[idx, "provider_state_NEW"] = state
    df.at[idx, "provider_zip_code_NEW"] = zip_code
    df.at[idx, "provider_country"] = country
    df.at[idx, "website_NEW"] = clinic_info["website"]

    df.at[idx, "phone_Match"] = str(row.get("phone_ORIGINAL")) == str(clinic_info["phone"])
    df.at[idx, "street_address_Match"] = str(row.get("street_address_ORIGINAL")) == street
    df.at[idx, "provider_city_Match"] = str(row.get("provider_city_ORIGINAL")) == city
    df.at[idx, "provider_state_Match"] = str(row.get("provider_state_ORIGINAL")) == state
    df.at[idx, "provider_zip_code_Match"] = str(row.get("provider_zip_code_ORIGINAL")) == zip_code
    df.at[idx, "specialty_Match"] = str(row.get("specialty_ORIGINAL")).lower().strip() == str(clinic_info["specialty"]).lower().strip()
    df.at[idx, "website_Match"] = str(row.get("website")) == str(clinic_info["website"])

    time.sleep(1) 
df.to_excel(output_file, index=False)
print(f"✅ Updated file saved: {output_file}")
