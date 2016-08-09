"""
decorator to add surrounding grave accents around
each element of specified argument
argument must be an iterable
arg_position is the position of arg if inputted as positional arg
arg_name is the name of arg if inputted as keyword arg
"""

from functools import wraps

def _add_grave_quotes(arg_position, arg_name):
    """sets input arg name"""

    def add_grave_wrapper(method):
        """wrapper to add grave quotes to elements of input argument"""

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            """wrapped to add grave quotes to input arg first"""

            #assume its a keyword arg first
            try:
                input_args = kwargs[arg_name]
            except KeyError:

                #if not, try positional arg.  Raise Exception if this case also fails
                try:
                    input_args = args[arg_position]
                except IndexError:
                    raise Exception('%s not a positional arg at position %d or keyword arg'%(arg_name, arg_position))
                else:
                    args = list(args)
                    args[arg_position] = ['`' + entry.strip('`') + '`' for entry in input_args]
                    args = tuple(args) #pylint: disable=redefined-variable-type

            #if keyword arg, replace keyword arg with quoted version
            else:
                kwargs[arg_name] = ['`' + entry.strip('`') + '`' for entry in input_args]

            #run main method
            return method(*args, **kwargs)

        return wrapped_method

    return add_grave_wrapper
