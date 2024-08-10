import asyncio
import random

from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import aiohttp
import yaml
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)


def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise


config = load_config()
token = "token"


async def main(date: str):
    logging.info(f"Starting main function for date: {date}")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto(
                f'https://grandtrain.ru/tickets/?step=1&month1=&month2=&year1=&year2=&STATION_FROM={config["stations"]["from"]}&STATION_TO={config["stations"]["to"]}&DATES%5B0%5D={date}&DATES%5B1%5D=&DATES%5B2%5D=&quantity%5Badult%5D=1&quantity%5Bchild%5D=0&quantity%5Bchild5%5D=0')

            await asyncio.sleep(random.randint(1, 10))
            trains = await page.query_selector_all('.train.train_seats.train-sort.train--tav')

            train_info_list = []
            for train in trains:
                train_number = await (await train.query_selector('.train_number_number')).inner_text()
                departure_time = await (await train.query_selector('.time')).inner_text()
                arrival_time = await (await train.query_selector('.arr .time')).inner_text()
                travel_time = await (await train.query_selector('.train_timing_dur .dur')).inner_text()
                train_cities = await (await train.query_selector('.train_cities')).inner_text()
                train_marks = {await mark.inner_text() for mark in await train.query_selector_all('.mark_item')}
                seat_availability = await (await train.query_selector('.train_cost')).inner_text()

                seats_inline = await train.query_selector('.seats.seats_inline')
                if seats_inline:
                    seat_details = []
                    for seat_item in await seats_inline.query_selector_all('.seats_item'):
                        seat_type = await (await seat_item.query_selector('.train_places_name')).inner_text()
                        seat_count = await (await seat_item.query_selector('.train_seats_count')).inner_text()
                        seat_cost = await (await seat_item.query_selector('.train_cost')).inner_text()
                        seat_details.append({
                            'seat_type': seat_type,
                            'seat_count': seat_count,
                            'seat_cost': seat_cost
                        })

                    filtered_seat_details = [
                        seat for seat in seat_details
                        if
                        any(position in seat['seat_count'] for position in config['seat_preferences']['seat_positions'])
                    ]
                    if filtered_seat_details and any(
                            seat['seat_count'].strip() not in ['0', 'Свободных мест нет'] and seat['seat_type'] in
                            config['seat_preferences']['seat_types'] for seat in filtered_seat_details):

                        button_row = await train.query_selector('.button_row')
                        if button_row:
                            button_link = await button_row.query_selector('a')
                            if button_link:
                                button_url = await button_link.get_attribute('href')
                                try:
                                    await send_telegram_message(config['telegram']['chat_id'],
                                                                train_number, departure_time, arrival_time, travel_time,
                                                                train_cities, train_marks, filtered_seat_details, date,
                                                                button_url)
                                except Exception as e:
                                    logging.exception(e)
                else:
                    seat_details = None

                train_info = {
                    'train_number': train_number,
                    'departure_time': departure_time,
                    'arrival_time': arrival_time,
                    'travel_time': travel_time,
                    'train_cities': train_cities,
                    'train_marks': train_marks,
                    'seat_availability': seat_availability,
                    'seat_details': seat_details
                }
                train_info_list.append(train_info)

            await browser.close()
            logging.info(f"Finished main function for date: {date}")
            return train_info_list
    except Exception as e:
        logging.error(f"Error in main function for date: {date}: {e}")
        return []


async def send_telegram_message(chat_id, train_number, departure_time, arrival_time, travel_time, train_cities,
                                train_marks, seat_details, date, button_url):
    try:
        message = f"🚂 *Информация о поезде* 🚂\n\n" \
                  f"📅 *Дата:* {date}\n" \
                  f"🔢 *Номер поезда:* {train_number}\n" \
                  f"🕒 *Время отправления:* {departure_time}\n" \
                  f"🕕 *Время прибытия:* {arrival_time}\n" \
                  f"⏱ *Время в пути:* {travel_time}\n" \
                  f"🛤 *Маршрут:* {train_cities}\n" \
                  f"ℹ️ *Описание поезда:* {', '.join(train_marks)}\n\n" \
                  f"💺 *Описание мест:*\n"
        for seat in seat_details:
            if seat['seat_type'] in config['seat_preferences']['seat_types']:
                message += f"  • *Тип:* {seat['seat_type']}, *Кол-во:* {seat['seat_count']}, *Цена:* {seat['seat_cost']}₽\n"

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown',
            'reply_markup': {
                'inline_keyboard': [
                    [{'text': '🎟 Выбрать места',
                      'url': f"https://grandtrain.ru/tickets/{config['stations']['from']}-{config['stations']['to']}/{date}/{button_url}"}]
                ]
            }
        }

        async with aiohttp.ClientSession() as session:
            logging.info(f"Sending Telegram message for train: {train_number}")
            await session.post(url, json=payload)
    except Exception as e:
        logging.error(f"Error sending Telegram message for train: {train_number}: {e}")


async def run_scheduler():
    try:
        logging.info("Starting scheduler")
        start_date = datetime.strptime(config['dates']['start_date'], '%Y-%m-%d')
        days_forward = config['dates']['days_forward']
        dates = [(start_date + timedelta(days=i)).strftime('%d.%m.%Y') for i in range(days_forward)]
        time_to_sleep_range = config['scheduler']['time_to_sleep_range']

        while True:
            tasks = [main(date) for date in dates]
            await asyncio.gather(*tasks)
            time_to_sleep = random.randint(time_to_sleep_range[0], time_to_sleep_range[1])
            logging.info(f"Sleeping for {time_to_sleep} seconds before next iteration")
            await asyncio.sleep(time_to_sleep)
    except Exception as e:
        logging.error(f"Error in scheduler: {e}")


if __name__ == "__main__":
    asyncio.run(run_scheduler())
