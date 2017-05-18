"""Schema config interfaces"""

class CompositeConfigInterface(object):

    @staticmethod
    def compose_config(config, builder):

        raise NotImplementedError()
