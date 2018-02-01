""" Results classes """

from etl_framework.etl_class import EtlClass

class Results(object):

    def __init__(self, results):
        """results : list of results objects"""

        self.results = results

    def set_up(self):
        """sets up each result"""

        for result in self.results:
            result.set_up()

    def tear_down(self):
        """tears down each result"""

        for result in self.results:
            result.tear_down()

    def actual_and_expected_results(self):
        """returns each actual and expected result pair"""

        for result in self.results:
            yield result.actual_and_expected_result()

class ResultInterface(EtlClass):

    def set_up(self):
        """ sets up"""

        self.schema.create_if_not_exists()

    def tear_down(self):
        """tears down"""

        self.schema.delete_if_exists()

    def raw_result(self):
        """helper method returns raw actual result of test"""

        raise NotImplementedError("Returns raw actual result of test")

    def actual_result(self):
        """returns formatted actual result"""

        raw_result = self.raw_result()

        if self.match_type == 'subset':
            # NOTE assumes all rows of expected result have same keys
            field_subset = set(self.expected_result[0].keys())
            result = [
                {
                    field: value for field, value in list(result.items())
                        if field in field_subset
                }
                for result in raw_result
            ]

        elif self.match_type == 'exact':

            result = raw_result

        else:
            raise Exception("Match type : {} not supported".format(
                self.match_type
            ))

        return result

    def actual_and_expected_result(self):
        """returns actual and expected result"""

        return self.actual_result(), self.expected_result
