import datetime
import json
import os
import socket
import threading

import google.cloud.logging as gclog


class IllegalLogSeverity(Exception):
    def __init__(self):
        pass

class CloudLogger:
    __LOGGER_NAME = 'logging'
    SEVERITIES = ['DEBUG', 'INFO', 'ERROR']

    def __init__(self, application_name, company_code='GENERALI',
                    organization_unit_code='', systemlog='', service_account=None):
        if service_account:
            self.__service_account = service_account
            self.__client = gclog.Client.from_service_account_json(self.__service_account)
        else:
            self.__client = gclog.Client()
        self.__cloud_logger = self.__client.logger(application_name)
        self.company_code = company_code
        self.organization_unit_code = organization_unit_code
        self.application_name = application_name
        self.hostname = socket.gethostname()
        self.clientip = socket.gethostbyname(socket.gethostname())
        self.processid = os.getpid()
        self.theradid = threading.get_ident()
        self.systemlog = systemlog

    def __format(self, record, levelname):
        data = {
            'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'level': levelname,
            'logger': CloudLogger.__LOGGER_NAME,
            'message': record,
            'companyCode': self.company_code,
            'organizationUnitCode': self.organization_unit_code,
            'hostname': self.hostname,
            'applicationName': self.application_name,
            'loggingForced': False,
            'threadID': self.theradid,
            'processID': self.processid,
            'SystemLog': self.systemlog
        }
        return data

    def publish_log(self, message, severity):
        if severity not in CloudLogger.SEVERITIES:
            raise IllegalLogSeverity('Logger has only ["DEBUG", "INFO", "ERROR"] severities.')
        payload = self.__format(message, severity)
        self.__cloud_logger.log_struct(payload, severity=severity)