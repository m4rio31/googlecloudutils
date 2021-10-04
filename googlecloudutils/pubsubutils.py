import json
import traceback

from google import api_core
from google.cloud import pubsub_v1


class PubSubUtils:
    """
    WARNING: This is a common class. If you modify this file copy it wherever it is used.
    """
    def __init__(self, project_id, service_account=None):
        self.__project_id = project_id
        if service_account:
            self.__service_account = service_account
            self.__publisher = pubsub_v1.PublisherClient.from_service_account_json(self.__service_account)
            self.__subscriber = pubsub_v1.SubscriberClient.from_service_account_json(self.__service_account)
        else:
            self.__publisher = pubsub_v1.PublisherClient()
            self.__subscriber = pubsub_v1.SubscriberClient()

    @property
    def project_id(self) -> str:
        return self.__project_id

    # ------------------------
    #     Public Methods
    # ------------------------
    def publish(self, topic_id, message) -> None:
        """
            Publish message on topic
        """
        topic_path = self.__publisher.topic_path(self.project_id, topic_id)
        self.__publisher.publish(topic=topic_path,
                         data=json.dumps(message).encode("utf-8"),
                         retry=PubSubUtils.__custom_retry())
        print(f"Published message to {topic_path}.")

    def start_subscription(self, subscription, 
                           callback, ack_before_callback=False) -> pubsub_v1.subscriber.futures.StreamingPullFuture:
        """
            Start a subscription thread
        """
        self.__logger.info(f"starting {subscription}")
        future = self.__subscribe(self.project_id, 
                                  subscription,
                                  self.__callback_parser(callback, ack_before_callback))
        return future

    # ------------------------
    #     Private Methods
    # ------------------------
    def __callback_parser(self, callback, ack_before_callback) -> None:
        def callback_wrapper(message):
            try:
                if ack_before_callback:
                    message.ack()
                json_msg = json.loads(message.data.decode('utf-8'))
                callback(json_msg)
                if not ack_before_callback:
                    message.ack()
                print("Message acknowledged")
            except Exception as ex:
                print(traceback.format_exc())
                print(f"Error on message callback (nack): {str(ex)}")
                if not ack_before_callback:
                    message.nack()
        return callback_wrapper

    @staticmethod
    def __custom_retry() -> api_core.retry.Retry:
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

    def __subscribe(self, subscription, callback) -> pubsub_v1.subscriber.futures.StreamingPullFuture:
        subscriber_path = self.__subscriber.subscription_path(self.__project_id,
                                                              subscription)
        try:
            future = self.__subscriber.subscribe(subscriber_path, callback)
        except Exception as ex:
            print(traceback.format_exc())
            print(f"EXCEPTION: cannot connect to {subscription}: {ex}")
            raise ex
        return future
