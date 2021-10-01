import logging
import os
from google.cloud import firestore
import ast
import datetime

from .get_args import get_args
from .common import get_json


class FirestoreUtils:

    def __init__(self, container_name):
        logging.basicConfig()
        self.logger = logging.getLogger(container_name)
        self.args = get_args(container_name, False)
        self.config = self.args[container_name]
        self.container_name = container_name

        self.firestore_client = firestore.Client()

        # self.insert_update_query = "INSERT INTO {nomeTabella} SET {update} ON DUPLICATE KEY UPDATE {update}"

    def upsert(self, message, collection, correlation, date_input = False):

        # "date_input" ,
        # "write_date" "

        new_message = get_json(self.config["{}_fields".format(collection)], message)
        now = datetime.datetime.now()
        new_message["write_date"] = now

        if date_input:
            new_message["date_input"] = now

        doc_id = ""
        for key in ast.literal_eval(self.config["{}_key".format(collection)]):
            key_temp = str(new_message[key]).replace("/", "|")
            if not doc_id:
                doc_id += key_temp
            else:
                doc_id = doc_id + "_" + key_temp

        doc = self.firestore_client.collection(collection).document(doc_id)
        doc.set(new_message, merge=True)

        self.logger.debug("Insert in {} document: {}".format(collection, new_message), extra=correlation)


        return "", True
