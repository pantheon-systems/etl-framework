"""some exceptions"""

class EnvironmentVariableNotSetException(Exception):
    """Raise this when an environment variable isn't set"""

    pass

class BadIteratorException(Exception):
    """raise this when component iterator doesnt iterate anything"""

    pass

class ConfigAttrNotSetException(Exception):
    """exception to throw if attribute not set"""

    pass

class ConfigNotSetException(Exception):
    """exception to throw if config not set"""

    pass

