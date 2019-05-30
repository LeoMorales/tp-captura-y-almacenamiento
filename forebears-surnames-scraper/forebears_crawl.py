from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from urllib.error import URLError
from pymongo import MongoClient
from urllib import parse
import datetime
import time
import logging
import os
import pandas as pd
import zlib
import pickle

DATA_SURNAMES_PATH = 'input/apellidos_chubut.csv'
TIME_BETWEEN_REQUEST = 30  # in seconds 
TIME_BETWEEN_REQUEST_WHEN_SERVER_NOT_RESPONDING = 60 * 5  # in seconds 
PROXY_HOST_PORT = os.environ.get('PROXY_INFO') if os.environ.get('PROXY') else '103.224.185.48:33553'
use_proxy = os.environ.get('USE_PROXY') if os.environ.get('USE_PROXY') else False

def setup_database(server='localhost', port=27017):
    logging.debug('Database setup: {} at {}'.format(server, port))
    client = MongoClient(server, port)
    db = client.forebears_database
    logging.debug('Database OK')
    return db.data_forebears

def compress_data(data):
    ''' Para descomprimir:
        descomprimido = pickle.loads(zlib.decompress(comprimido))
    '''
    return zlib.compress(pickle.dumps(data))

def get_data(apellido, num_retries=2):
    logging.debug('Searching data of: {}'.format(apellido))
    datetime_log = datetime.datetime.now().isoformat()
    surnames_url = 'https://forebears.io/surnames/{}'.format(
        parse.quote(apellido.lower()))
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'referrer': 'https://google.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Pragma': 'no-cache'
    }
    
    req = Request(url=surnames_url, headers=headers)
    if use_proxy:
        logging.debug('Using proxy: {}'.format(PROXY_HOST_PORT))
        req.set_proxy(PROXY_HOST_PORT, 'https')
    try:
        html = urlopen(req).read()
    except HTTPError as e:
        print("{} - The server returned an HTTP error".format(datetime_log))
        logging.debug("{} - The server returned an HTTP error".format(datetime_log))
        return None, 'HTTPError'
    except URLError as e:
        print("{} - The server could not be found!".format(datetime_log), e.reason)
        logging.debug("{} - The server could not be found!".format(datetime_log), e.reason)
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # recursively retry 5xx HTTP errors
                return (get_data(apellido, num_retries-1))
        return None, 'URLError'
    else:
        print("{} - Respuesta OK".format(datetime_log))
        logging.debug("{} - Respuesta OK".format(datetime_log))
        return compress_data(html), 'OK'


def cargar_apellidos():
    data = pd.read_csv(DATA_SURNAMES_PATH, header=None)
    return data[0]


def request_ack(url, headers):
    datetime_log = datetime.datetime.now().isoformat()
    req = Request(url=url, headers=headers)
    if use_proxy:
        req.set_proxy(PROXY_HOST_PORT, 'https')

    try:
        html = urlopen(req).read()
    except HTTPError as e:
        print("The server returned an HTTP error")
        return False
    except URLError as e:
        print("The server could not be found!", e.reason)
        if hasattr(e, 'code') and 500 <= e.code < 600:
            print("5xx HTTP errors")
        return False
    else:
        print("{} - ACK".format(datetime_log))
        return True

def wait_for_server():
    datetime_log = datetime.datetime.now().isoformat()
    print("{} - Waiting for server".format(datetime_log))
    logging.debug("{} - Waiting for server".format(datetime_log))
    forebears_url = 'https://forebears.io/surnames/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'
    }
    while not request_ack(forebears_url, headers):
        print('waiting {} seconds'.format(
            TIME_BETWEEN_REQUEST_WHEN_SERVER_NOT_RESPONDING))
        logging.debug('waiting {} seconds'.format(
            TIME_BETWEEN_REQUEST_WHEN_SERVER_NOT_RESPONDING))
        time.sleep(
            TIME_BETWEEN_REQUEST_WHEN_SERVER_NOT_RESPONDING)


def main():
    print('MAIN started')
    logging.debug('MAIN')
    
    port = os.environ.get('MONGODB_PORT')
    port = int(port) if port else 27017
    logging.debug('MongoDB port: {}'.format(port))

    coleccion_forebears = setup_database(port=port)
    apellidos = cargar_apellidos()
    logging.debug('Apellidos: {}'.format(len(apellidos)))
    logging.debug(
        'Primer apellido: {}'.format(
            apellidos[0] if len(apellidos) > 0 else '-'))

    # MAIN LOOP:
    print('MAIN LOOP start')
    logging.debug('MAIN LOOP start')
    for apellido in apellidos:
        data, result = get_data(apellido)
        if result == 'URLError':
            wait_for_server()
            data, result = get_data(apellido)
        doc_apellido = {
            "apellido": apellido,
            "html_data_compressed": data,
            "result": result,
            "recolectado": datetime.datetime.utcnow()
        }
        coleccion_forebears.insert_one(doc_apellido)
        logging.debug('\n{} - Recolectado: {} - Data size: {} - Result: {}'.format(
            datetime.datetime.now().isoformat(),
            apellido,
            len(data) if data else 0,
            result))

        time.sleep(TIME_BETWEEN_REQUEST)
        
def test():
    print('TEST')
    print('Es util saber la ruta en al cual trabaja el container')
    print(os.getcwd())
    
    print('Y si los archivos se copiaron correctamente')
    print(os.listdir())

    logging.debug('TEST Started...')
    
    print('Las variables de entorno se cargan correctamente?')
    port = os.environ.get('MONGODB_PORT')
    port = int(port) if port else 27017
    print('MongoDB port: {}'.format(port))
    logging.debug('MongoDB port: {}'.format(port))

    logging.debug('TEST finished.')
    print('TEST END.')


if __name__ == '__main__':
    datetime_log = datetime.datetime.now().isoformat()
    datetime_log = datetime_log.replace('.','_').replace(':','-')
    logging.basicConfig(
        filename='logs/scraping_event-{}.log'.format(datetime_log),
        level=logging.DEBUG)

    main()
    #test()



