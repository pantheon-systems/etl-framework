"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class GcloudMixin(object):
    """parses configuration files"""

    GCLOUD_PROJECT_ID_ATTR = 'gcloud_project_id'

    @check_config_attr_default_none
    def get_gcloud_project_id(self):
        """stuff"""

        return self.config[self.GCLOUD_PROJECT_ID_ATTR]

    def set_gcloud_project_id(self, gcloud_project_id):
        """stuff"""

        self.config[self.GCLOUD_PROJECT_ID_ATTR] = gcloud_project_id
