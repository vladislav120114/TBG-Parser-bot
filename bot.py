import json
import sqlite3
import telebot
from telebot import types
from threading import Thread
from datetime import datetime, UTC
from parser import main as run_parser
import time
from openpyxl import Workbook
import os
import hashlib

password = "c3b6a5552c14932785bbe94f9c4e58cba3d91b4b4937c0b9721cad2ee462a1e0"

TOKEN = '7136085502:AAG5t-gbQBqiVLU6Tm035Qc5RE_9MMaRyIA'
DB_PATH = 'aircraft_data.db'

bot = telebot.TeleBot(TOKEN)
monitoring = {}


def hash_it(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_aircraft_types():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0].replace('aircraft_', '').replace('_', '-') for table in tables if table[0].startswith('aircraft_')]


def get_new_records(aircraft_type, last_check):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = f"aircraft_{aircraft_type.replace('-', '_')}"
    cursor.execute(f"SELECT * FROM {table_name} WHERE time > ? ORDER BY time ASC", (last_check,))
    records = cursor.fetchall()
    conn.close()
    return records


def monitor_updates(user_id, aircraft_type):
    last_check = monitoring[user_id]['last_check']

    while user_id in monitoring:
        new_records = get_new_records(aircraft_type, last_check)
        if new_records:
            for record in new_records:
                text = f'Самолет типа {aircraft_type} с ICAO кодом {record[2]} отметился в {record[1]}'
                markup = types.InlineKeyboardMarkup()
                detail = types.InlineKeyboardButton("Показать полную информацию", callback_data=f'detail_{record[0]}')
                full = types.InlineKeyboardButton("Выгрузить все выходы", callback_data=f'full_{record[2]}')
                markup.add(detail)
                markup.add(full)
                bot.send_message(user_id, text, reply_markup=markup)
            last_check = new_records[-1][1]
            monitoring[user_id]['last_check'] = last_check
        time.sleep(10)


@bot.message_handler(commands=['start'])
def start(message):
    global users
    try:
        with open('users.json', 'r') as f:
            users = json.load(f)
    except:
        users = {
            'arr': []
        }
    if hash_it(str(message.from_user.id)) in users['arr']:
        keyboard = types.InlineKeyboardMarkup()
        aircraft_types = get_aircraft_types()
        arr = []
        for i in range(len(aircraft_types)):
            if (i+1) % 4 != 0:
                arr.append(types.InlineKeyboardButton(aircraft_types[i], callback_data=f'type_{aircraft_types[i]}'))
            else:
                arr.append(types.InlineKeyboardButton(aircraft_types[i], callback_data=f'type_{aircraft_types[i]}'))
                keyboard.row(*arr)
                arr = []
            if len(aircraft_types) - i - 1 == 0:
                keyboard.row(*arr)
        bot.send_message(message.chat.id, "Выберите тип самолета:", reply_markup=keyboard)
    else:
        bot.send_message(message.chat.id, 'Вы не авторизованны в системе. Введите пароль')

@bot.message_handler(func=lambda message: True)
def check_pass(message):
    if hash_it(str(message.text)) == password:
        bot.send_message(message.chat.id, 'Пароль верный')
        users['arr'].append(hash_it(str(message.from_user.id)))
        with open('users.json', 'w') as f:
            json.dump(users, f, ensure_ascii=False, indent=4)
        start(message)
    else:
        bot.send_message(message.chat.id, 'Пароль неверный')

@bot.callback_query_handler(func=lambda call: call.data.startswith('type_'))
def type_callback_query(call):
    keyboard = types.InlineKeyboardMarkup()
    aircraft_type = call.data.split('_')[1]
    download = types.InlineKeyboardButton('Выгрузить таблицу', callback_data=f'download_{aircraft_type}')
    update = types.InlineKeyboardButton('Выводить новые записи', callback_data=f'update_{aircraft_type}')
    keyboard.add(download, update)
    bot.send_message(call.message.chat.id, f"Вы выбрали тип: {aircraft_type}.", reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith('update_'))
def update_callback_query(call):
    keyboard = types.ReplyKeyboardMarkup(True, True)
    keyboard.add('/stop')
    aircraft_type = call.data.split('_')[1]
    bot.send_message(call.message.chat.id, f"Вывод самолетов типа {aircraft_type} начался.", reply_markup=keyboard)
    monitoring[call.message.chat.id] = {'aircraft_type': aircraft_type, 'last_check': datetime.now(UTC).isoformat()}
    monitor_thread = Thread(target=monitor_updates, args=(call.message.chat.id, aircraft_type))
    monitor_thread.start()


@bot.callback_query_handler(func=lambda call: call.data.startswith('download_'))
def download_callback_query(call):
    aircraft_type = call.data.split('_')[1]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = f"aircraft_{aircraft_type.replace('-', '_')}"
    cursor.execute(f"SELECT * FROM {table_name}")
    record = cursor.fetchall()
    conn.close()
    wb = Workbook()
    ws = wb.active
    ws.append(["id", "time", "icao", "rego", "type", "desc", "own_call", "msg"])
    for i in record:
        try:
            ws.append(i)
        except:
            print(i)
            continue
    wb.save(f"{aircraft_type}.xlsx")
    f = open(f"{aircraft_type}.xlsx", "rb")
    bot.send_document(call.message.chat.id, f)
    f.close()
    os.remove(f"{aircraft_type}.xlsx")


@bot.callback_query_handler(func=lambda call: call.data.startswith('detail_'))
def detail_callback_query(call):
    record_id = int(call.data.split('_')[1])
    aircraft_type = call.message.text.split(' ')[2]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = f"aircraft_{aircraft_type.replace('-', '_')}"
    cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (record_id,))
    record = cursor.fetchone()
    conn.close()

    if record:
        full_text = (
            f"Time: {record[1]}\n"
            f"ICAO: {record[2]}\n"
            f"Rego: {record[3]}\n"
            f"Type: {record[4]}\n"
            f"Desc: {record[5]}\n"
            f"Own/Call: {record[6]}\n"
            f"Msg: {record[7]}"
        )
        bot.send_message(call.message.chat.id, full_text)


@bot.callback_query_handler(func=lambda call: call.data.startswith('full_'))
def full_callback_query(call):
    icao = call.data.split('_')[1]
    aircraft_type = call.message.text.split(' ')[2]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = f"aircraft_{aircraft_type.replace('-', '_')}"
    cursor.execute(f"SELECT * FROM {table_name} WHERE icao = ?", (icao,))
    record = cursor.fetchall()
    conn.close()
    wb = Workbook()
    ws = wb.active
    ws.append(["id", "time", "icao", "rego", "type", "desc", "own_call", "msg"])
    for i in record:
        try:
            ws.append(i)
        except:
            print(i)
            continue
    wb.save(f"{icao}.xlsx")
    f = open(f"{icao}.xlsx", "rb")
    bot.send_document(call.message.chat.id, f)
    f.close()
    os.remove(f"{icao}.xlsx")


@bot.message_handler(commands=['stop'])
def stop(message):
    if message.chat.id in monitoring:
        del monitoring[message.chat.id]
    start(message)


if __name__ == '__main__':
    parser_thread = Thread(target=run_parser)
    parser_thread.start()
    bot.infinity_polling(none_stop=True)