"""decorator to check instance attribute is set (i.e. evaluates to true)"""

from functools import wraps

def _check_attr_set(attr):
    """sets attribute for wrapper"""
    def check_attr_wrapper(method):
        """wrapper to check config attribute is set"""

        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            """wrapped method to check config set first"""

            if getattr(self, attr):
                return method(self, *args, **kwargs)
            else:
                raise Exception('Cant run method. Set config attribute \'%s\' first!'%(attr, ))

        return wrapped_method

    return check_attr_wrapper

