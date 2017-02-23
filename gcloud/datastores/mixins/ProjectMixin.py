"""Mixin for PubsubClient to have subscription attribute"""

class ProjectMixin(object):
    """mixin to ack Pubsub messages"""

    def __init__(self, project_name, *args, **kwargs):
        """creates instance"""

        # NOTE: project_name argument should be renamed project_id
        # but this requires changing the name in all of the pubsub classes
        self.project_id = project_name
        self.project = 'projects/' + project_name

        super(ProjectMixin, self).__init__(*args, **kwargs)

