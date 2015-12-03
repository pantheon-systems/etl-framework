"""parses configuration and returns useful things"""
#pylint: disable=relative-import

class FiltersMixin(object):
    """stuff"""

    def pre_filter_row(self, row):
        """stuff"""

        return self.config.get_pre_filter()(row)

    def filter_row(self, row):
        """stuff"""

        return self.config.get_filter()(row)

    def post_filter_row(self, row):
        """stuff"""

        return self.config.get_post_filter()(row)

