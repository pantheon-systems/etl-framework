"""parses configuration and returns useful things"""
#pylint: disable=relative-import

class GcloudMixin(object):
    """parses configuration files"""

    @property
    def gcloud_project_id(self):
        """stuff"""

        return self.config.get('gcloud_project_id')

    @gcloud_project_id.setter
    def gcloud_project_id(self, gcloud_project_id):
        """stuff"""

        self.config['gcloud_project_id'] = gcloud_project_id
