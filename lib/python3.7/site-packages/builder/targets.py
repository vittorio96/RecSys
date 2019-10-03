""" This document will hold the way that targets are defined

Likely these targets will be abstract target's, non abstract targets will
maybe be defined in something that looks a little more config like
"""
import errno
import fnmatch
import glob
import os

import abc

class Target(object):
    """A target class is one that can express certain attributes that are
    common to all targets

    These attributes are
        mtime: the last modificaiton time of a target
        exists: whether or not the target exists

    These two attributes require a way of getting them individually and a way
    to get them in bulk

    args:
        unexpanded_id: The id that the file takes in the rule_dependency_graph
        unique_id: The id that this file will take in the build_graph
        build_context: The context that the node currently has, not what was
            used to expand it
        config: A dictionary of properties that the target may use as a config
    """
    def __init__(self, unexpanded_id, unique_id, build_context, config=None):
        self.unexpanded_id = unexpanded_id
        self.unique_id = unique_id
        self.build_context = build_context
        self.config = config

        self.cached_mtime = False
        self.mtime = None

        self.expanded_directions = {"up": False, "down": False}

    def invalidate(self):
        """Sets the mtime value to not cached"""
        self.cached_mtime = False

    @abc.abstractmethod
    def do_get_mtime(self):
        """Returns the current value of mtime. Must return None if the target
        does not exist.
        """

    def get_exists(self):
        """returns whether or not the target has been built before
        on a local file system it is simply the exists, in impala it might
        be whether or not something is found in a where statement

        args:
            cached: True if the cached value is wanted
        """
        return self.get_mtime() is not None

    def get_mtime(self):
        """returns whether or not the target has been built before
        on a local file system it is simply the mtime, in impala it might
        be a value stored elsewhere by a command

        args:
            cached: True if the cached value is wanted
        """
        if self.cached_mtime:
            return self.mtime
        else:
            self.mtime = self.do_get_mtime()
            self.cached_mtime = True
            return self.mtime

    def set_mtime(self, mtime):
        """Set cached mtime value

            mtime: Number of seconds since the epoch (float)
        """
        self.mtime = mtime
        self.cached_mtime = True

    def is_cached(self):
        return self.cached_mtime

    def get_id(self):
        """ Returns a unique ID for this target
        """
        return self.unique_id

    @staticmethod
    @abc.abstractmethod
    def get_bulk_exists_mtime(targets):
        """Gets the existance and mtime values for the targets in bulk

        Returns:
            A dictionary with the form
            {
                "unique_id1": {
                    "exists": True/False
                    "mtime": number
                },
                "unique_id2": {
                    "exists": True/False
                    "mtime": number
                }, ...
            }
        """
        exists_mtime_dict = {}
        for target in targets:
            local_path = target.unique_id
            mtime = target.do_get_mtime()
            exists = mtime is not None
            exists_mtime_dict[local_path] = {
                    "exists": exists,
                    "mtime": mtime,
            }
            target.mtime = mtime
            target.cached_mtime = True
        return exists_mtime_dict


class LocalFileSystemTarget(Target):
    """A local file system target is one that lives on the local
    filesystem

    The mtime is retrieved doing a standard stat on the file
    The existance value is equivalent to it's return value to exists
    """
    @staticmethod
    def non_cached_mtime(local_path):
        """Gets the non cached mtime of a the local_path

        is static so get_bulk_exists_mtime can use it

        returns:
            The value of the mtime if the file exists, otherwise None
        """
        try:
            mtime = os.stat(local_path).st_mtime
        except OSError as oserror:
           if oserror.errno == errno.ENOENT:
                mtime = None
           else:
                raise oserror
        return mtime

    @staticmethod
    def get_bulk_exists_mtime(targets):
        """Gets all the exists and mtimes for the local paths and returns them
        in a dict. Just as efficient as normal mtime and exists
        """
        exists_mtime_dict = {}
        for target in targets:
            local_path = target.unique_id
            mtime = LocalFileSystemTarget.non_cached_mtime(local_path)
            exists = mtime is not None
            exists_mtime_dict[local_path] = {
                    "exists": exists,
                    "mtime": mtime,
            }
            target.mtime = mtime
            target.cached_mtime = True
        return exists_mtime_dict

    def do_get_mtime(self):
        """Returns the value of the mtime of the file as reported
        by the local filesystem

        Also caches the existance value of the file

        Returns:
            The value of the mtime if the file exists, otherwise None
        """
        mtime = LocalFileSystemTarget.non_cached_mtime(self.unique_id)

        return mtime


class GlobLocalFileSystemTarget(Target):
    """Used to get information about glob targets."""
    @staticmethod
    def non_cached_mtime(pattern):
        """Gets the maximum mtime of the files that match the glob pattern

        Is statis so get_bulk_exists_mtime can use it

        returns:
            The value of the maximum mtime if at least one file exists that
            matches the patterns, otherwise None
        """
        max_mtime = None
        file_glob = glob.glob(pattern)
        for file_path in file_glob:
            mtime = LocalFileSystemTarget.non_cached_mtime(file_path)
            if max_mtime is None or mtime > max_mtime:
                max_mtime = mtime

        return max_mtime

    def do_get_mtime(self):
        """The mtime retrieved corresponds to the largest mtime matching the
        pattern
        """
        mtime = GlobLocalFileSystemTarget.non_cached_mtime(self.unique_id)

        return mtime

    @staticmethod
    def get_bulk_exists_mtime(targets):
        """Uses the non chached mtime to retrieve mtimes in bulk.
        Just as efficient as getting the values individually
        """
        patterns = [x.unique_id for x in targets]
        exists_mtime_dict = {}
        for pattern in patterns:
            mtime = GlobLocalFileSystemTarget.non_cached_mtime(pattern)
            exists = mtime is not None
            exists_mtime_dict[pattern] = {
                    "exists": exists,
                    "mtime": mtime,
            }
        return exists_mtime_dict


