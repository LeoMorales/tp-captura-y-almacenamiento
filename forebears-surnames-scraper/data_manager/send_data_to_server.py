# coding: utf-8
from pymongo import MongoClient
import json
import os
import datetime
import zlib
import pickle


def main():
	print('Working dir:', os.getcwd())

	client_local = MongoClient()
	db = client_local.forebears_database  # data_forebears
	surnames_collections_local = db.data_forebears
	print('Count collection LOCAL:', surnames_collections_local.estimated_document_count())


	client_server = MongoClient('localhost', 30001)
	db_server = client_server.forebears_database  # data_forebears
	surnames_collections_server = db_server.data_forebears

	print('Count collection:', surnames_collections_server.estimated_document_count())


	print('START COPY...')
	for data_surname in surnames_collections_local.find({}):
	    surnames_collections_server.insert_one(data_surname)
	print('COPY FINISHED.')
	print('Count collection server:', surnames_collections_server.estimated_document_count())


if __name__ == '__main__':
	main()