from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from urllib.parse import urlparse
import time
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

options = Options()
chrome_driver_path = r"C:\path\to\chromedriver-win64\chromedriver-win64\chromedriver.exe"
driver = webdriver.Chrome(service=Service(chrome_driver_path), options=options)
driver.get("https://nfcollective.org/find-a-doctor/")

last_count = 0

while True:
    time.sleep(2)
    driver.execute_script("document.querySelector('.storepoint-results-container').scrollTo(0, 100000);")
    time.sleep(2)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    current_count = len(soup.select('.storepoint-results-container .storepoint-location'))
    
    if current_count == last_count:
        print("All locations loaded.")
        break
    last_count = current_count

html = driver.page_source
driver.quit()

def fetch_nfc_data(source):
    soup = BeautifulSoup(source, 'html.parser')
    try:
        nfc_data = {
            "id": [],
            "RunDateTime": [],
            "hospital": [],
            "specialty_ORIGINAL": [],
            "phone_ORIGINAL": [],
            "street_address_ORIGINAL": [],
            "provider_city_ORIGINAL": [],
            "provider_state_ORIGINAL": [],
            "provider_zip_code_ORIGINAL": [],
            "website": [],
            "physicians_1_ORIGINAL": [],
            "facility_type_ORIGINAL": [],
        }
        container_info = soup.select('.storepoint-results-container .storepoint-location')
        for info in container_info:
            nfc_data["id"].append(info.get('data-id', ''))
            nfc_data["RunDateTime"].append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            hospital_name = info.find('div', class_='storepoint-name')
            nfc_data["hospital"].append(hospital_name.text.strip() if hospital_name else '')

            specialty = info.select_one(
                '.storepoint-text-field:-soup-contains("Specialty") .storepoint-text-field-value')
            nfc_data["specialty_ORIGINAL"].append(specialty.text.strip() if specialty else '')

            phone = info.find('a', class_='storepoint-phone-link')
            nfc_data["phone_ORIGINAL"].append(phone.text.strip() if phone else '')

            address_block = info.find('div', class_='storepoint-address')
            if address_block:
                address_lines = list(address_block.stripped_strings)
                address_lines = address_lines[0].split('\n') if address_lines else []

                if len(address_lines) >= 2:
                    nfc_data["street_address_ORIGINAL"].append(address_lines[0])
                    city_state_zip = address_lines[-1].rsplit(' ', 2)
                    if len(city_state_zip) == 3:
                        city, state, zip_code = city_state_zip
                        nfc_data["provider_city_ORIGINAL"].append(city)
                        nfc_data["provider_state_ORIGINAL"].append(state)
                        nfc_data["provider_zip_code_ORIGINAL"].append(zip_code)
                    else:
                        nfc_data["provider_city_ORIGINAL"].append('')
                        nfc_data["provider_state_ORIGINAL"].append('')
                        nfc_data["provider_zip_code_ORIGINAL"].append('')
                else:
                    nfc_data["street_address_ORIGINAL"].append('')
                    nfc_data["provider_city_ORIGINAL"].append('')
                    nfc_data["provider_state_ORIGINAL"].append('')
                    nfc_data["provider_zip_code_ORIGINAL"].append('')
            else:
                nfc_data["street_address_ORIGINAL"].append('')
                nfc_data["provider_city_ORIGINAL"].append('')
                nfc_data["provider_state_ORIGINAL"].append('')
                nfc_data["provider_zip_code_ORIGINAL"].append('')

            website_link = info.find('a', class_='storepoint-website-button')
            parsed_url = urlparse(website_link['href']) if website_link else None
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}" if parsed_url else ''
            nfc_data['website'].append(base_url)

            physician_field = info.select_one(
                '.storepoint-text-field:-soup-contains("Clinic Director") .storepoint-text-field-value')
            nfc_data["physicians_1_ORIGINAL"].append(physician_field.text.strip() if physician_field else '')

            tags_groups = info.find('div', class_='storepoint-tag-group-1ugr6gf7myg')
            tags_labels = tags_groups.find_all('div', class_='storepoint-tag-label')
            tags = [tag.text.strip() for tag in tags_labels if tag.text.strip()]
            nfc_data["facility_type_ORIGINAL"].append(', '.join(tags) if tags else '')

        return nfc_data
    except Exception as e:
        print(f"An error occurred while fetching NFC data: {e}")
        return None


data = fetch_nfc_data(html)

df = pd.DataFrame(data)
print(df)
df.to_excel("nfcollective_data1.xlsx", index=False)
