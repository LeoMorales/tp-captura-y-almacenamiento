# coding: utf-8

''' Extrae data de la base mongodb

Solo contiene los metodos para comprobar que la base de datos
se encuentre cargada correctamente --> recorrer los documentos
y mostrar la data luego de descomprimir el html guardado.
'''
import pymongo
import gzip
import io
import pickle
import zlib
from pymongo import MongoClient
from bs4 import BeautifulSoup
from pprint import pprint


def setup_mongodb(server='localhost', port=27017):
    client = MongoClient(server, port)
    db = client.forebears_database  # data_forebears
    return db.data_forebears


def extract_data(content):
    ''' Recibe una cadena html y extrae la infomacion de paises en los
    que encontramos el apellido y los apellidos que son similares al apellido
    al cual pertenece el contenido
    '''
    def extract_countries_incidence(soup_obj):
        surnames_data = soup_obj.select(".nation-table tbody tr")
        countries = []
        for surname_row in surnames_data:
            country = dict()
            #renglon['place'], renglon['incidence'], renglon['frequency'], renglon['rank_in_area'] = [col.text for col in fila.find_all('td')]
            columns = surname_row.findAll('td')
            country['place'] = columns[0].text
            country['incidence'] = columns[1].text
            country['frequency'] = columns[2].text
            country['rank_in_area'] = columns[3].text
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


def decompress_content(data):
    gzip_f = gzip.GzipFile(
        fileobj=io.BytesIO(
            pickle.loads(zlib.decompress(data))))
    return str(gzip_f.read())


def test():
    surnames_collection = setup_mongodb()
    test_doc = surnames_collection.find_one(
        { 'apellido': 'ABOUZED'})
    descomprimido = pickle.loads(
        zlib.decompress(test_doc['html_data_compressed']))
    gzip_f = gzip.GzipFile(fileobj=io.BytesIO(descomprimido))
    content = str(gzip_f.read())
    pprint(extract_data(content))


def main():
    surnames_collection = setup_mongodb()
    for i, surnames_data in enumerate(surnames_collection.find()):
        if i == 4: break
        pprint(surnames_data['apellido'])
        pprint(surnames_data['result'])
        if surnames_data['result'] == 'OK':
            pprint(
                extract_data(
                    decompress_content(
                        surnames_data['html_data_compressed'])))


if __name__ == '__main__':
    main()