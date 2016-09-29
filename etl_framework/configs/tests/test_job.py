""" test the environment config """

import os

import unittest

from etl_framework.configs.job import JobConfig


class JobConfigTestCases(unittest.TestCase):
    """ class for test cases """

    JOB_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/job.json'
    )

    def setUp(self):

        self.job_config = JobConfig.create_from_filepath(
            self.JOB_CONFIG_FILEPATH
        )

    def test_get_environment_configuration(self):
        """ stuff """

        # This is determined by the fixtures/job.json config
        # and should be the value of "environment" key
        expected_output = {
            "config_dir": "fixtures",
            "config_filename": "environment.json"
        }

        output = self.job_config.get_environment_configuration()

        self.assertEqual(output, expected_output)

    def test_get_environment_configuration_filepath(self):
        """ stuff """

        # This is determined by the fixtures/job.json config
        expected_filepath = 'fixtures/environment.json'

        filepath = self.job_config.get_environment_configuration_filepath()

        self.assertEqual(filepath, expected_filepath)
