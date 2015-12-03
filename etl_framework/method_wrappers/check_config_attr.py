"""decorator to see if config attribute exists or throws error"""

from functools import wraps

class ConfigAttrNotSetException(Exception):
    """exception to throw if attribute not set"""

    pass

class ConfigNotSetException(Exception):
   """exception to throw if config not set"""

   pass

def check_config_attr(method):
    """wrapper to check config attribute is set"""

    @wraps(method)
    def wrapped_method(self, *args, **kwargs):
        """wrapped method to check config set first"""
        try:
            return method(self, *args, **kwargs)
        except KeyError as e:
            raise ConfigAttrNotSetException(str(e))
        except AttributeError as e:
            raise ConfigNotSetException(set(e))

    return wrapped_method
