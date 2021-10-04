import json
import os
import pandas as pd
from io import BytesIO

from google.cloud import storage


class GCSUtils:
    """
    WARNING: This is a common class. If you modify this file copy it wherever it is used.
    """

    def __init__(self, bucket_name, service_account=None):
        if service_account:
            self.__service_account = service_account
            self.__client = storage.Client.from_service_account(self.__service_account)
        else:
            self.__client = storage.Client()
        self.__bucket_name = bucket_name
        self.__bucket = self.__client.get_bucket(self.__bucket_name)

        if not self.__bucket_name:
            raise ValueError("missing 'bucket_name' variable")

    @property
    def bucket_name(self):
        return self.__bucket_name
    @property
    def bucket(self):
        return self.__bucket

    # ------------------------
    #     Public Methods
    # ------------------------
    def create_empty_directory(self, path) -> bool:
        '''
            Create empty directory in bucket.
        '''
        if not path.endswith("/"):
            # self.logger.error(traceback.format_exc(), extra=correlation)
            raise ValueError("Directory must ends with trailing slash. actual: ", path)
        blob = self.__bucket.blob(path)
        try:
            blob.upload_from_string("")
            # self.logger.debug("Create directory: {0} ".format(path), extra=correlation)
        except Exception as err:
            print(f'ERROR create_empty_directory -> {err}')
        return True

    def delete(self, file_path) -> bool:
        '''
            Delete element from bucket.
        '''
        blob = self.__bucket.blob(file_path)
        try:
            blob.delete()
        except Exception as err:
            print(f'ERROR delete_from_bucket -> {err}')
            return False
        return True

    def explore_bucket(self, prefix=None) -> list:
        '''
            Create a list of elements in the bucket.
            It is possible to choose a common prefix to filter the elements.
        '''
        names_list = []
        blobs = self.__bucket.list_blobs(prefix=prefix)
        for elem in blobs:
            blob_name = os.path.splitext(elem.name.split('/')[-1])[0]
            names_list.append(blob_name)
        return names_list

    def get_csv(self, path) -> pd.core.frame.DataFrame:
        '''
            Download CSV file from bucket storing it into a Pandas DataFrame
        '''
        df = pd.read_csv(self.__download_bytes(path))
        return df

    def get_json(self, path) -> dict:
        '''
            Download JSON file from bucket
        '''
        json_file = json.loads(self.__download_string(path))
        return json_file
        
    def save(self, blob_path, source, data_type=None) -> bool:
        '''
            Save data to bucket.
            It is possible to select among csv, json, string format or even a file.
        '''
        try:
            if data_type == 'csv':
                if isinstance(source, pd.core.frame.DataFrame):
                    # Pandas DataFrame handling
                    source = source.to_csv(index=False)
                self.__save_csv(blob_path, source)
            elif data_type == 'json':
                self.__save_json(blob_path, source)
            elif data_type == 'doc':
                self.__save_file(blob_path, source)
            elif data_type is None:
                self.__save_string(blob_path, source)
            else:
                raise ValueError('Invalid value for "data_type" flag.')
            # self.logger.debug("Saved file: {0} ".format(final_path), extra=correlation)
        except Exception as err:
            print(f'ERROR save_to_bucket -> {err}')
            return False
        return True

    # ------------------------
    #     Private Methods
    # ------------------------
    def __save_csv(self, output_name, source) -> None:
        blob = self.__bucket.blob(output_name)
        blob.upload_from_string(data=str(source), content_type='text/csv')

    def __save_string(self, output_name, source) -> None:
        blob = self.__bucket.blob(output_name)
        blob.upload_from_string(data=str(source), content_type='text/plain')

    def __save_json(self, output_name, source) -> None:
        blob = self.__bucket.blob(output_name)
        if isinstance(source, dict):
            blob.upload_from_string(data=json.dumps(source),
                                    content_type='application/json')
        elif isinstance(source, str):
            if GCSUtils.__is_json(source):
                blob.upload_from_string(data=source,
                                        content_type='application/json')
            else:
                raise ValueError('Invalid JSON. No file has been saved.')
        else:
            raise TypeError('Invalid source format. Dict or string requested.')

    def __save_file(self, output_name, source) -> None:
        blob = self.__bucket.blob(output_name)
        blob.upload_from_filename(source)

    @staticmethod
    def __is_json(myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True

    def __download_bytes(self, file_path) -> BytesIO:
        blob = self.__bucket.blob(file_path)
        bytes_file = BytesIO(blob.download_as_bytes())
        return bytes_file

    def __download_string(self, file_path) -> str:
        blob = self.__bucket.blob(file_path)
        str_file = blob.download_as_string()
        return str_file
