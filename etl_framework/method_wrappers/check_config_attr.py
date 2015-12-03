"""decorator to see if config attribute exists or throws error"""

from functools import wraps

from etl_framework.Exceptions import ConfigAttrNotSetException,\
                                    ConfigNotSetException

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
