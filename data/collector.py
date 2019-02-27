import regex
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from sqlite3worker import Sqlite3Worker
import os


db_name = "contents"


def extract_urls(answers):
    reg = r'<a\s+(?:[^>]\s+)?href=([\"\'])([^>\"\']+microsoft\.com.*?)\1'
    seen = set()
    for answer in answers:
        for url in regex.finditer(reg, answer):
            if url.group(2) in seen:
                continue
            yield url.group(2)
            seen.add(url.group(2))


class Collector:
    def init_db(self):
        self.sql_worker.execute('''CREATE TABLE docs
             (url text, content text)''')
        self.sql_worker.execute('''CREATE TABLE errors
             (url text, statuscode int)''')


    def __init__(self):
        db_fullname = "{}.db".format(db_name)
        if os.path.isfile(db_fullname):
            os.remove(db_fullname)
        if os.path.isfile("{}-journal".format(db_fullname)):
            os.remove("{}-journal".format(db_fullname))
        self.sql_worker = Sqlite3Worker(db_fullname)
        self.init_db()


    def __del__(self):
        self.sql_worker.close()


    def get_session_with_retries(
        self,
        session=None,
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
    ):
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        session = session or requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session


    def fetch_doc(self, session, url):
        try:
            with session.get(url, allow_redirects=True) as response:
                if response.encoding is None:
                    response.encoding = 'utf-8'
                if response.status_code != 200:
                    print("Error in {0} {1}".format(url, response.status_code))
                    self.save_error_url(url, response.status_code)
                    return
                
                print('Success getting {}'.format(url))
                self.save_content(url, response.text)
        except Exception as e:
            msg = 'Exception thrown during getting data from {}. Message: {}'.format(url, e)
            print('Exception thrown during getting data from {}.'.format(url))
            self.save_error_url(url, message = msg)
        except (KeyboardInterrupt, SystemExit):
            raise


    async def load_docs_asynchronous(self, urls):
        with ThreadPoolExecutor(max_workers=500) as executor:
            with self.get_session_with_retries() as session:
                loop = asyncio.get_event_loop()
                tasks = [loop.run_in_executor(executor, self.fetch_doc, *(session, url)) for url in urls ]
                for _ in await asyncio.gather(*tasks):
                    pass


    def save_content(self, url, content):
        self.sql_worker.execute("INSERT INTO docs VALUES (?, ?)", (url, content))

    
    def save_error_url(self, url, statuscode = "", message = ""):
        self.sql_worker.execute("INSERT INTO errors VALUES (?, ?)", (url, statuscode))