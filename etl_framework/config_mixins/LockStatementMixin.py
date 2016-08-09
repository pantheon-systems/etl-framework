"""parses configuration and returns useful things"""
#pylint: disable=relative-import

class LockStatementMixin(object):
    """requires LoaderMixin"""

    @staticmethod
    def create_postgres_lock_statement(table):
        """returns postgres lock statement"""

        return 'LOCK TABLE {0} IN SHARE ROW EXCLUSIVE MODE;'.format(table)

    def get_postgres_lock_statement(self):
        """returns statement to insert data"""

        return self.create_postgres_lock_statement(self.get_loader_table())
