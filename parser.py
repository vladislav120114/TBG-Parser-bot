import time
import sqlite3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

urls = [
    "https://tbg.airframes.io/dashboard/milusaf",
    "https://tbg.airframes.io/dashboard/milraf",
    "https://tbg.airframes.io/dashboard/milPacific",
    "https://tbg.airframes.io/dashboard/milapac",
    "https://tbg.airframes.io/dashboard/milamer",
    "https://tbg.airframes.io/dashboard/milemea"
]

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=chrome_options)


def parse_page(url):
    driver.get(url)
    WebDriverWait(driver, timeout=60).until(EC.presence_of_element_located((By.XPATH, '//tbody')))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    rows = soup.select('tbody tr')
    data = []
    for row in rows:
        record = {}
        cells = row.find_all('td')
        for cell in cells:
            try:
                cell = cell.text.split(': ')
                key = cell[0].replace('/', '_')
                value = cell[1]
                record[key] = value
            except:
                continue
        record['time'] = datetime.strptime(record['time'], '%H:%M:%SZ %d-%m-%Y').isoformat()
        record['msg'] = record['msg'].replace('\\', ' ')
        data.append(record)
    return data


def create_database():
    conn = sqlite3.connect('aircraft_data.db')
    return conn


def create_aircraft_table(conn, aircraft_type):
    cursor = conn.cursor()
    table_name = f"aircraft_{aircraft_type.replace('-', '_')}"  # Имя таблицы для типа самолета
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            icao TEXT,
            rego TEXT,
            type TEXT,
            desc TEXT,
            own_call TEXT,
            msg TEXT,
            UNIQUE(time, icao, rego, type)
        )
    ''')
    conn.commit()


def save_to_database(conn, data):
    cursor = conn.cursor()
    for record in data:
        aircraft_type = record.get('Type').replace('-', '_')
        create_aircraft_table(conn, aircraft_type)
        table_name = f"aircraft_{aircraft_type}"

        cursor.execute(f'''
            SELECT COUNT(*) FROM {table_name}
            WHERE time = ? AND icao = ? AND rego = ? AND type = ?
        ''', (record.get('time'), record.get('ICAO'), record.get('Rego'), record.get('Type')))

        if cursor.fetchone()[0] == 0:
            cursor.execute(f'''
                INSERT INTO {table_name} (time, icao, rego, type, desc, own_call, msg)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (record.get('time'), record.get('ICAO'), record.get('Rego'), record.get('Type'), record.get('desc'),
                  record.get('own_call'), record.get('msg')))

    conn.commit()


def continuous_parsing(interval):
    conn = create_database()
    try:
        while True:
            all_data = []
            for url in urls:
                page_data = parse_page(url)
                all_data.extend(page_data)
            save_to_database(conn, all_data)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Парсинг остановлен пользователем.")
    finally:
        driver.quit()
        conn.close()


def main():
    try:
        continuous_parsing(60)
    except:
        main()

if __name__ == '__main__':
    main()