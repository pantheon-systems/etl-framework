"""
decorator to run a method check (i.e. self.authenticate()).
method check should raise error if doesnt pass.
method check should need no arguments
"""

from functools import wraps

def _run_check(check_method_name):
    """sets check method for wrapper"""

    def run_check_wrapper(method):
        """wrapper to run check before calling main method"""

        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            """wrapped method to check config set first"""

            #runs check method before running main method
            check_method = getattr(self, check_method_name)
            check_method()

            #run main method
            return method(self, *args, **kwargs)

        return wrapped_method

    return run_check_wrapper
