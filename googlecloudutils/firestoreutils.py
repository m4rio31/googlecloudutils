import datetime

from google.cloud import firestore


class FirestoreUtils:

    def __init__(self, service_account=None):
        if service_account:
            self.__service_account = service_account
            self.__client = firestore.Client.from_service_account_json(self.__service_account)
        else:
            self.__client = firestore.Client()

    # ------------------------
    #     Public Methods
    # ------------------------
    def select_docs(self, collection_name, single_occurrence=False, **kwargs) -> list:
        '''
            Select docs list from collection.
            In case the flag 'single_occurrence' is setup to True, the returning list
            will contain only one element because the document is uniquely binded to the key.
        '''
        if single_occurrence is True:
            doc_id = list(kwargs.keys())[0]
            query_response = self.__client.collection(collection_name).document(kwargs[doc_id])
            response_list = [query_response.get().to_dict()]
        else:
            query = self.__format_query(collection_name, kwargs)
            query_response = query.stream()
            response_list = self.__transform_to_dict(query_response)
        return response_list


    def upsert(self, message, collection_name, collection_keys, collection_fields) -> None:
        '''
            Insert/update info in document.
        '''
        new_message = FirestoreUtils.__get_json(collection_fields, message)
        now = datetime.datetime.now()
        new_message["write_date"] = now

        # if date_input:
        new_message["date_input"] = now

        doc_id = ""
        for key in collection_keys:
            key_temp = str(new_message[key]).replace("/", "|")
            if not doc_id:
                doc_id += key_temp
            else:
                doc_id = doc_id + "_" + key_temp

        doc = self.__firestore_client.collection(collection_name).document(doc_id)
        try:
            doc.create(new_message)
            print(f"Insert in {collection_name} document: {doc_id}")
        except Exception as ex:
            print(f"{ex}: Existing document, do merge in {collection_name} document: {doc_id}")
            del new_message["date_input"]
            doc.set(new_message, merge=True)

    # ------------------------
    #     Private Methods
    # ------------------------
    @staticmethod
    def __get_json(key_list, result):
        final_json = {}
        for key in key_list:
            if key in result:
                final_json[key] = result[key]
            else:
                print(f"Key {key} not present.")
                continue
        return final_json

    def __format_query(self, collection_name, keys_dict):
        query = self.__client.collection(collection_name)
        for key in keys_dict.keys():
            query = query.where(key, "==", keys_dict[key]).stream()
        return query

    @staticmethod
    def __transform_to_dict(query_response):
        response_list = []
        for elem in query_response:
            response_list.append(elem.to_dict())
        return response_list