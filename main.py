import pandas as pd
import regex as re
import requests
import csv
import asyncio
from data.collector import Collector, extract_urls


def main():
    df = pd.read_csv("../data.csv")
    urls = extract_urls(df['AnswerBody'])
    collector = Collector()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(collector.load_docs_asynchronous(urls))
    loop.run_until_complete(future)


if __name__ == "__main__":
    main()