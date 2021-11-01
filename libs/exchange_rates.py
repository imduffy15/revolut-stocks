from io import StringIO
import csv
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from dateutil.parser import parse
import json
import os
import decimal
import logging

logger = logging.getLogger("exchange_rates")

decimal.getcontext().rounding = decimal.ROUND_HALF_UP

from libs import EXCHANGE_RATES_BASE_URL


def get_exchange_rates():
    exchange_rates = {}
    with open("fxrates.csv") as fp:
      reader = csv.reader(fp)
      for row in reader:
        if row[0] and row[1]:
          date = parse(row[0])
          fxrate = row[1]
          exchange_rates[date] = decimal.Decimal(fxrate)

    return exchange_rates


def find_last_published_exchange_rate(exchange_rates, search_date):
    return min(exchange_rates.keys(), key=lambda date: abs(date - search_date))


def populate_exchange_rates(statements):
    exchange_rates = get_exchange_rates()

    for statement in statements:
        if statement["trade_date"] in exchange_rates:
            statement["exchange_rate"] = exchange_rates[statement["trade_date"]]
            statement["exchange_rate_date"] = statement["trade_date"]
            continue

        statement["exchange_rate_date"] = find_last_published_exchange_rate(exchange_rates, statement["trade_date"])
        statement["exchange_rate"] = exchange_rates[statement["exchange_rate_date"]]
