"""Schema config interfaces"""

class CompositeConfigInterface(object):

    def compose(self, builder):

        raise NotImplementedError()
