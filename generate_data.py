"""
Functions to generate sample data """

import csv
import json
import random
from dataclasses import dataclass
from faker import Faker
from pathlib import Path
from typing import List
import datetime

DATA_DIR = "./data"
fake = Faker()


@dataclass
class RandomDataGenerator:
    ticket_count: int = 2
    order_count: int = 2
    customer_count: int = 2
    year_count: int = 3
    first_year: int = datetime.datetime.now().year - 3

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    _events = [
        {"code": "CHL-ARS", "name": "Chelsea vs Arsenal", "date": "5-May-yyyy"},
        {"code": "F1-SILV", "name": "Silverstone", "date": "30-Jan-yyyy"},
        {"code": "BTH-SRC", "name": "Bath vs Saracens", "date": "Jan-30-yyyy"}
    ]

    @property
    def years(self) -> List[int]:
        first_year = self.first_year
        last_year = first_year + self.year_count
        return range(first_year, last_year)

    @property
    def events(self) -> List[dict]:
        events = []
        for year in self.years:
            year_events = [self._event_factory(e, year) for e in self._events]
            events.extend(year_events)
        return events

    def generate_all_data(self):
        self.generate_ticket_data()
        self.generate_customer_data()

    def generate_ticket_data(self) -> None:
        data = []
        for r in range(1, self.ticket_count + 1):
            event = random.choice(self.events)
            row_data = {
                "ticket_id": r,
                "order_id": random.choice(range(1, self.order_count)),
                "customer_id": random.choice(range(1, self.customer_count)),
                "quantity": random.randint(1, 5),
                "net_sales": random.random() * 100,
                "event_code": event["code"],
                "event_name": event["name"],
                "event_date": event["date"],
                "event_season": event["season"]
            }
            data.append(row_data)

        self._save_data_to_csv_file(data, "tickets.csv", delimiter="|")

    def generate_customer_data(self) -> None:
        data = []
        for r in range(1, self.customer_count + 1):
            row_data = {
                "CustomerIdentity": r,
                "FirstName": fake.first_name(),
                "LASTNAME": fake.last_name(),
                "contact_details": {
                    "postcode": fake.postcode(),
                    "phone": fake.phone_number()
                },
                "Customer Title": fake.prefix()
            }
            data.append(row_data)

        self._save_data_to_json_file(data, "customers.json")

    @staticmethod
    def _event_factory(event: dict, year: int) -> dict:
        season = f"{year}/{year + 1}"
        event["season"] = season
        event["date"] = event["date"].replace("yyyy", str(year))
        return event

    @staticmethod
    def _save_data_to_csv_file(data: List[dict], file_name: str, delimiter: str=",") -> None:
        file_path = f"{DATA_DIR}/{file_name}"
        with open(file_path, "w+", newline="") as f:
            headers = data[0].keys()
            dict_writer = csv.DictWriter(f=f, fieldnames=headers, delimiter=delimiter)
            dict_writer.writeheader()
            dict_writer.writerows(data)

    @staticmethod
    def _save_data_to_json_file(data: List[dict], file_name: str) -> None:   
        file_path = f"{DATA_DIR}/{file_name}"
        with open(file_path, "w+", encoding="utf-8") as f:
            json.dump(data, f)


if __name__ == '__main__':
    data_generator = RandomDataGenerator(ticket_count=1000, order_count=400, customer_count=200)
    data_generator.generate_all_data()
