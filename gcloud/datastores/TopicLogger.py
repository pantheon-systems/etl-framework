"""Logging class to write pubsub request messages to disk"""

import os
import json

from etl_framework.gcloud.datastores.utils.SubscriberMessage import SubscriberMessage
from etl_framework.gcloud.datastores.PubsubSubscriber import PubsubSubscriber

class TopicLogger(PubsubSubscriber):
    """writes pubsub request messages to disk"""

    #PUBLISH_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    DEFAULT_FILENAME = 'UNKNOWN'

    def __init__(self, log_directory, ack_batch_size=100, *args, **kwargs):
        """creates instance"""

        self.log_directory = log_directory
        self.ack_batch_size = ack_batch_size

        #private attributes used to keep track of logging files
        self._current_file_obj = None

        super(TopicLogger, self).__init__(*args, **kwargs)

    def iter_pull_messages_from_log(self, filenames):
        """
        creates subscribe message objects from files in log
        filenames is list of base filenames to read from
        """

        for filename in filenames:
            filepath = os.path.join(self.log_directory, filename)

            with open(filepath, 'r') as file_obj:
                for line in file_obj:
                    yield SubscriberMessage(json.loads(line.strip()))

    def iter_log(self):
        """writes files to log"""

        ack_ids = []

        try:
            for received_message in self.iter_pull(auto_ack=False):

                #keep track of ack_ids since we aren't auto_acking
                ack_ids.append(received_message.get('ackId'))

                current_file_obj = self._get_file_obj(received_message)
                current_file_obj.write(json.dumps(received_message) + '\n')

                #we ack if there are more than ack_batch_size number of ack_ids
                if ack_ids and len(ack_ids) >= self.ack_batch_size:
                    self.ack_messages(ack_ids)
                    ack_ids = []

        finally:

            #make sure to ack remaining messages
            if ack_ids:
                self.ack_messages(ack_ids)

            #make sure to close file_obj if necessary
            if self._current_file_obj:
                self._current_file_obj.close()
                self._current_file_obj = None

    def _get_file_obj(self, received_message):
        """
        returns the file obj for given filename
        also handles closing current existing file_obj if necessary
        """

        new_filepath = self._get_filepath(received_message)

        if self._current_file_obj:

            if self._current_file_obj.name == new_filepath:
                pass
            else:
                self._current_file_obj.close()
                self._current_file_obj = open(new_filepath, 'a')
        else:
            self._current_file_obj = open(new_filepath, 'a')

        return self._current_file_obj

    def _get_filepath(self, received_message):
        """creates log filepath for received_message"""

        filename = self.DEFAULT_FILENAME

        pubsub_message = received_message.get('message')
        if pubsub_message:
            publish_time = pubsub_message.get('publishTime')
            if publish_time:
                #filename should be date of publishTime
                filename = publish_time.split('T')[0]

        return os.path.join(self.log_directory, filename)


