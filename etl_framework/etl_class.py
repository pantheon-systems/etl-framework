"""Base EtlClass that all EtlClasses should inherit"""

class EtlClass(object):

    def __init__(self, config):

        self.config = config

    def __setattr__(self, key, value):
        """Set attribute on config if not in EtlClass object"""

        if key == "config":
            self.__dict__[key] = value
        elif hasattr(self.config, key):
            setattr(self.config, key, value)
        else:
            self.__dict__[key] = value

    def __getattr__(self, key):
        """Get attribute on config if not in EtlClass object"""

        # Get attribute if Config doesnt exist
        # we don't need a special call to super here because getattr is only
        # called when an attribute is NOT found in the instance's dictionary
        config = self.config
        return getattr(config, key)

