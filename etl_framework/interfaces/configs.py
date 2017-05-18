"""Schema config interfaces"""

class CompositeConfigInterface(object):

    def compose_config(self, builder):

        raise NotImplementedError()
