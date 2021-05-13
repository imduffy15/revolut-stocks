from io import StringIO
import csv
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import json
import os
import decimal
import logging

logger = logging.getLogger("exchange_rates")

decimal.getcontext().rounding = decimal.ROUND_HALF_UP

from libs import EXCHANGE_RATES_BASE_URL


def query_exchange_rates(date):
    logger.debug(f"Obtaining exchange rate for {date}")

    try:
        resp = requests.get(EXCHANGE_RATES_BASE_URL + date.strftime("%Y-%m-%d"), params={"base":"USD", "symbols": "EUR"}).json()
        return { date: decimal.Decimal(resp['rates']['EUR']) }
    except Exception:
        logging.exception(f"Unable to get exchange rates. Please, try again later.")
        raise SystemExit(1)

def get_exchange_rates(first_date, last_date):
    first_date -= relativedelta(months=1)  # Get one extra month of data to ensure there was a published exchange rate
    exchange_rates = {}
    while True:
        curr_fs_date = first_date
        exchange_rates.update(query_exchange_rates(curr_fs_date))
        first_date = curr_fs_date + relativedelta(days=1)

        if first_date > last_date:
            break

    return exchange_rates


def find_last_published_exchange_rate(exchange_rates, search_date):
    return min(exchange_rates.keys(), key=lambda date: abs(date - search_date))


def populate_exchange_rates(statements, use_bnb):
    first_date = statements[0]["trade_date"]
    last_date = statements[-1]["trade_date"]

    exchange_rates = {}
    exchange_rates = get_exchange_rates(first_date, last_date)

    for statement in statements:
        if statement["trade_date"] in exchange_rates:
            statement["exchange_rate"] = exchange_rates[statement["trade_date"]]
            statement["exchange_rate_date"] = statement["trade_date"]
            continue

        statement["exchange_rate_date"] = find_last_published_exchange_rate(exchange_rates, statement["trade_date"])
        statement["exchange_rate"] = exchange_rates[statement["exchange_rate_date"]]
