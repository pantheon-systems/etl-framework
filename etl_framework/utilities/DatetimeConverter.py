"""class to convert datetime values"""

import datetime

class DatetimeConverter(object):
    """stuff"""

    _EPOCH_0 = datetime.datetime(1970, 1, 1)

    def __init__(self):
        """stuff"""

        pass

    @staticmethod
    def get_tomorrow():
        """stuff"""

        return datetime.datetime.today() + datetime.timedelta(days=1)

    @staticmethod
    def get_yesterday():

        return datetime.datetime.today() - datetime.timedelta(days=1)

    @classmethod
    def get_timestamp(cls, datetime_obj):
        """helper method to return timestamp fo datetime object"""

        return (datetime_obj - cls._EPOCH_0).total_seconds()

    @classmethod
    def get_tomorrow_timestamp(cls):
        """stuff"""

        return cls.get_timestamp(cls.get_tomorrow())

    @classmethod
    def get_yesterday_timestamp(cls):

        return cls.get_timestamp(cls.get_yesterday())
