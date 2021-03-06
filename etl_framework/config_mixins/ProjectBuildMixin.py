"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr

class ProjectBuildMixin(object):
    """parses configuration files"""

    PROJECT_BUILD_ATTR = "project_build"

    @check_config_attr
    def get_project_build(self):
        """gets configurations"""

        return self.config[self.PROJECT_BUILD_ATTR]

    def set_project_build(self, project_build):
        """sets configurations"""

        self.config[self.PROJECT_BUILD_ATTR] = project_build

