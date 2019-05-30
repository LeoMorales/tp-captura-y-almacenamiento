# coding: utf-8
#
# Elasticsearch loader
# 
# Lee de la base y extrae la informacion del html comprimido y carga elasticsearch

import os
import pymongo
import gzip
import io
import pickle
import zlib
from pymongo import MongoClient
from bs4 import BeautifulSoup
from pprint import pprint
from datetime import datetime
from elasticsearch import Elasticsearch
import requests
import time
import json


def get_working_collection(server='localhost', port=27017):
    ''' setup_mongodb
    
        Returns:
            - MongoDB collection: data_forebears
    '''
    client = MongoClient(server, port)
    db = client.forebears_database  # data_forebears
    return db.data_forebears


def extract_data(content):
    ''' Recibe una cadena html y extrae la infomacion de paises en 
    los que encontramos el apellido y los apellidos que son similares 
    al apellido al cual pertenece el contenido.
    '''
    def extract_countries_incidence(soup_obj):
        surnames_data = soup_obj.select(".nation-table tbody tr")
        countries = []
        for surname_row in surnames_data:
            country = dict()
            #renglon['place'], renglon['incidence'], renglon['frequency'], renglon['rank_in_area'] = [col.text for col in fila.find_all('td')]
            columns = surname_row.findAll('td')
            country['place'] = columns[0].text
            country['incidence'] = float(columns[1].text.replace(',',''))
            country['frequency'] = columns[2].text
            country['rank_in_area'] = float(columns[3].text.replace(',',''))
            countries.append(country)

        return countries

    def extract_phonetically_similars(soup_obj):
        phonetically_similars = []
        similarities = soup_obj.select("#similar-table tbody tr")
        for similar in similarities:
            columnas = similar.findAll('td')
            similitud = dict()
            similitud['surname'] = columnas[0].text
            similitud['similarity'] = columnas[1].text
            similitud['incidence'] = columnas[2].text
            similitud['prevalency'] = [span_i.get('title') for span_i in columnas[3].findAll('span') if span_i.has_attr('title')]
            phonetically_similars.append(similitud)
        
        return phonetically_similars

    bs = BeautifulSoup(content, 'html.parser')
    return {
        'countries': extract_countries_incidence(bs),
        'similarities': extract_phonetically_similars(bs)
    }


def decompress_binary_data(data):
    ''' Descomprime el html comprimido en la base '''
    gzip_f = gzip.GzipFile(
        fileobj=io.BytesIO(
            pickle.loads(zlib.decompress(data))))
    return str(gzip_f.read())


def decompress_data(data):
    return pickle.loads(zlib.decompress(data))


def list_documents():
    surnames_collection = get_working_collection(port=30001)
    for i, surnames_data in enumerate(surnames_collection.find()):
        pprint(surnames_data['apellido'])
        pprint(surnames_data['result'])
        if surnames_data['result'] == 'OK':
            pprint(
                extract_data(decompress_data(surnames_data['html_data_compressed'])))
        break


def main():
    list_documents()
    

def send_to_es():
    ''' Crea el indice elasticsearch utilizando el cliente Python '''
    # Cargar en es:
    es = Elasticsearch()
    es.indices.delete(index='surnames-index', ignore=[400, 404])

    surnames_collection = get_working_collection(port=30001)
    for i_id, surname_data in enumerate(surnames_collection.find({}), 1):
        if surname_data['result'] == 'OK':
            to_insert_es = dict()
            to_insert_es['surname'] = surname_data['apellido']
            aux = extract_data(decompress_binary_data(surname_data['html_data_compressed']))
            to_insert_es['countries'] = aux['countries']
            to_insert_es['similarities'] = aux['similarities']
            pprint('Insertando {}'.format(to_insert_es['surname']))
                        
            res = es.index(index="surnames-index", doc_type='surname', id=i_id, body=to_insert_es)
            pprint(res['result'])

def send_to_es_phonetic():
    ''' Crea el indice fonetico utilizando Request! '''
    
    def create_phonetic_index():
        ''' Crea el index '''
        headers = {'Content-Type': 'application/json'}
        url = "http://localhost:9200/phonetic"
        data = {
            "settings": {
                "analysis": {
                    "filter": {
                        "dbl_metaphone": {
                            "type":    "phonetic",
                            "encoder": "double_metaphone"
                        }
                    },
                    "analyzer": {
                        "dbl_metaphone": {
                            "tokenizer": "standard",
                            "filter":    "dbl_metaphone"
                        }
                    }
                }
            }
        }
        pprint(
                requests.put(
                    url,
                    headers=headers,
                    data=json.dumps(data)).text)
        time.sleep(2)
    
    def mapping_phonetic():
        ''' Mapea el index '''
        headers = {'Content-Type': 'application/json'}
        url = "http://localhost:9200/phonetic/_mapping/phonetic"
        data = {
            "properties": {
                "surname": {
                    "type": "text",
                    "fields": {
                        "phonetic": {
                            "type":     "text",
                            "analyzer": "dbl_metaphone"
                        }
                    }
                }
            }
        }
        pprint(
                requests.put(
                    url,
                    headers=headers,
                    data=json.dumps(data)).text)
        time.sleep(2)
    
    # setup phonetic index:
    create_phonetic_index()
    mapping_phonetic()
    
    headers = {'Content-Type': 'application/json'}
    surnames_collection = get_working_collection(port=30001)
    for i_id, surname_data in enumerate(surnames_collection.find({}), 1):
        if surname_data['result'] == 'OK':
            url = "http://localhost:9200/phonetic/phonetic/{}".format(i_id)
            data = {'surname': surname_data['apellido']}
            pprint('Insertando {}'.format(surname_data['apellido']))
            pprint(
                requests.put(
                    url,
                    headers=headers,
                    data=json.dumps(data)).text)  # https://stackoverflow.com/questions/31542318/converting-a-curl-to-python-requests-put
            
if __name__ == '__main__':
    #send_to_es()
    send_to_es_phonetic()
    print('Insercion finalizada')

