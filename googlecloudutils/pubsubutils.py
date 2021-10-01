import json
import os

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv('PROJECT_ID')
AUTH_FILE = os.getenv('AUTH_FILE')


class QueuePublisher:
    def __init__(self, topic_id):
        self.__publisher = pubsub_v1.PublisherClient \
                                    .from_service_account_json(AUTH_FILE)
        self.__topic_path = self.__publisher.topic_path(PROJECT_ID, topic_id)

    @property
    def publisher(self):
        return self.__publisher
    @property
    def topic_path(self):
        return self.__topic_path

    def publish_message(self, data) -> bool:
        json_data = json.dumps(data)
        try:
            self.__publisher.publish(self.__topic_path,
                                     data=bytes(json_data, 'utf-8'))
            return True
        except:
            return False

class QueueSubscriber:
    def __init__(self, sub_id):
        self.__subscriber = pubsub_v1.SubscriberClient \
                                    .from_service_account_json(AUTH_FILE)
        self.__subscription = f'projects/{PROJECT_ID}/subscriptions/{sub_id}'

    @property
    def subscriber(self):
        return self.__subscriber
    @property
    def subscription(self):
        return self.__subscription

    def subscribe(self, callback):
        sub = self.__subscriber.subscribe(self.__subscription,
                                callback=callback)
        return sub

import json
import logging
import os
import traceback

from .correlation_object import CorrelationObject

from google import api_core
from google.cloud import pubsub_v1

from .get_args import get_args


class PubSubUtils:
    """
    WARNING: This is a common class. If you modify this file copy it wherever it is used.
    """
    def __init__(self, container_name):
        logging.basicConfig()
        self.logger = logging.getLogger(container_name)

        self.config = get_args(container_name, False)[container_name]
        self.project_id = os.getenv('PROJECT_ID', self.config.get('project_id'))

    # ------------------------
    #     Public Methods
    # ------------------------
    #def create_correlation(self, message):
    #    try:
    #        correlation_obj = CorrelationObject()
    #        correlation = {'correlation': correlation_obj}
    #        correlation_obj.set_id_comunicazione(message.get(self.config["json_key_id_seq"], None))
    #        return correlation
    #    except Exception as ex:
    #        self.logger.warning(traceback.format_exc())
    #        self.logger.warning(f"Exception: cannot create CorrelationObject: {ex}")
    #        return None

    def publish(self, topic_id, message, correlation=None):
        producer = pubsub_v1.PublisherClient()
        topic_path = producer.topic_path(self.project_id, topic_id)
        producer.publish(topic=topic_path,
                         data=json.dumps(message).encode("utf-8"),
                         retry=PubSubUtils.__custom_retry())
        self.logger.info(f"Published message to {topic_path}.",
                         extra=correlation)

    def start_subscription(self, subscription, callback, ack_before_callback=False):
        """Start a subscription thread

            @param: subscription id
            @param: callback function
            @param: ack_before_callback to bet set as True for long life processes (without timeouts)
        """
        self.logger.info(f"starting {subscription}")
        future = self.__subscribe(self.project_id, 
                                  subscription,
                                  self.__callback_parser(callback, ack_before_callback))
        return future


    # ------------------------
    #     Private Methods
    # ------------------------
    def __callback_parser(self, callback, ack_before_callback):
        def callback_wrapper(message):
            try:
                if ack_before_callback:
                    message.ack()
                json_msg = json.loads(message.data.decode('utf-8'))
                #correlation = self.create_correlation(json_msg)
                callback(json_msg)
                if not ack_before_callback:
                    message.ack()
                self.logger.debug("Message acknowledged", extra=correlation)
            except Exception as ex:
                self.logger.error(traceback.format_exc())
                self.logger.error(f"Error on message callback (nack): {str(ex)}")
                if not ack_before_callback:
                    message.nack()

        return callback_wrapper

    @staticmethod
    def __custom_retry():
        return api_core.retry.Retry(
            initial=0.1,  # seconds (default: 0.1)
            maximum=60.0,  # seconds (default: 60.0)
            multiplier=1.3,  # default: 1.3
            deadline=60.0,  # seconds (default: 60.0)
            predicate=api_core.retry.if_exception_type(
                api_core.exceptions.Aborted,
                api_core.exceptions.DeadlineExceeded,
                api_core.exceptions.InternalServerError,
                api_core.exceptions.ResourceExhausted,
                api_core.exceptions.ServiceUnavailable,
                api_core.exceptions.Unknown,
                api_core.exceptions.Cancelled,
            ),
        )

    def __subscribe(self, project_id, subscription, callback):
        subscriber = pubsub_v1.SubscriberClient()
        subscriber_path = subscriber.subscription_path(project_id, subscription)

        try:
            future = subscriber.subscribe(subscriber_path, callback)
        except Exception as ex:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"EXCEPTION: cannot connect to {subscription}: {ex}")
            raise ex

        return future
