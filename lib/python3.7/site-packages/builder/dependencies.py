"""All dependency types go here. The dependency types will be accessed with
getattr so there is no need for a key value list. In the future to allow for
dependencies to be defined elsewhere, it might be usefull for a config file
"""

class Dependency(object):
    """Holds the dependency function and the unique_id"""
    def __init__(self, func, unique_id, kind):
        self.func = func
        self.unique_id = unique_id
        self.kind = kind

def get_dependencies(dependency_type):
    """Returns the function with the name of dependency_type"""
    return globals()[dependency_type]

def depends(dependencies):
    """Returns true if all the dependencies exists, else false"""
    return all([x.get_exists() for x in dependencies])

def depends_one_or_more(dependencies):
    """Returns true if any of the dependencies exist, else false"""
    return any([x.get_exists() for x in dependencies])
