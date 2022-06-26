import requests
import sched, time

from datetime import date
from xml.etree import ElementTree

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

import psycopg2

def get_today_usd():
# Функция получает сегодняшние данные курса доллара с сайта ЦБ РФ
    today = date.today().strftime("%d/%m/%Y")
    response = requests.get('https://www.cbr.ru/scripts/XML_daily.asp?date_req=' + today)
    xml_tree = ElementTree.fromstring(response.content)
    
    for child in xml_tree:
        if child.attrib["ID"] == "R01235": # код доллара в получаемом файле
            for tag in child.findall('Value'):
                return float(tag.text.replace(",", "."))


def get_data_from_gsheets(): 
    # Выгрузка данных из таблицы
    CREDENTIALS_FILE = 'service_account.json'
    spreadsheet_id = '1dbq54IAlu-kQsWjCth4DtAT6PBQpY3nxULuNgKhWXZ4'

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)

    data = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Sheet1'
    ).execute()["values"]

    return data

def setup_database(data):
    # Устанавливаем подключение к базе данных и создаем таблицу 
    connection = psycopg2.connect(user="test_user",
                       password="password",
                       database="test_db",
                       host="127.0.0.1");

    cursor = connection.cursor()
    connection.autocommit = True

    cursor.execute(
        """
        DROP TABLE IF EXISTS orders;
        CREATE TABLE orders(
        order_id integer PRIMARY KEY,
        number integer,
        cost_usd float,
        delivery_date date,
        cost_rub float)
        """
    )

    # заполняем таблицу данными, включая новый столбец со стоимостью в рублях
    usd_rub_course = get_today_usd()

    for record in data[1:]:
        order_id = int(record[0])
        number = int(record[1])
        cost_usd = float(record[2])
        delivery_date = record[3]
        cost_rub = cost_usd * usd_rub_course
            
        cursor.execute(
            """INSERT INTO orders 
            (order_id, 
             number, 
             cost_usd, 
             delivery_date, 
             cost_rub) 
             VALUES (%s, %s, %s, %s, %s)""", 
            (order_id, number, cost_usd, delivery_date, cost_rub))

    return connection, cursor



if __name__ == "__main__":

    schedule = sched.scheduler(time.time, time.sleep)

    def run():
        schedule.enter(3000, 1, run)
        data = get_data_from_gsheets()
        connection, cursor = setup_database(data)

    run()
    schedule.run()

