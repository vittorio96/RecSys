"""Used to test the various functions used to implement the
build api specifically the target ones
"""

import fnmatch
import unittest

import mock

import builder.targets


class LocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """
    @staticmethod
    def mock_mtime_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_mtime(path):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            if path not in file_dict:
                raise OSError(2, "No such file or directory")
            mock_stat = mock.Mock()
            mock_stat.st_mtime = file_dict[path]
            return mock_stat
        return mock_mtime


    def test_non_cached_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
                "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            mtime1 = (builder.targets.LocalFileSystemTarget
                        .non_cached_mtime(file1.unique_id))
            mtime2 = (builder.targets.LocalFileSystemTarget
                        .non_cached_mtime(file2.unique_id))

        self.assertEqual(mtime1, 1)
        self.assertIsNone(mtime2)

    def test_get_exists(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            exists1 = file1.get_exists()
            exists2 = file2.get_exists()

        self.assertTrue(exists1)
        self.assertFalse(exists2)

    def test_get_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            mtime1 = file1.get_mtime()
            mtime2 = file2.get_mtime()

        self.assertEqual(mtime1, 1)
        self.assertIsNone(mtime2)

    def test_get_bulk_exists_mtime(self):
        # given
        path1_file = "local_path/1"
        path1 = builder.targets.LocalFileSystemTarget(path1_file, path1_file,
                                                      {})
        path2_file = "local_path/2"
        path2 = builder.targets.LocalFileSystemTarget(path2_file, path2_file,
                                                      {})

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1_file, path1_file, build_context)

        mtime_fetcher = file1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_mtime):
            mtimes_exists = mtime_fetcher([path1, path2])
            file1_exists = mtimes_exists[path1_file]["exists"]
            file2_exists = mtimes_exists[path2_file]["exists"]
            file1_mtime = mtimes_exists[path1_file]["mtime"]
            file2_mtime = mtimes_exists[path2_file]["mtime"]

        self.assertTrue(file1_exists)
        self.assertFalse(file2_exists)
        self.assertEqual(file1_mtime, 1)
        self.assertEqual(file2_mtime, None)



class GlobLocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """

    @staticmethod
    def mock_glob_list_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_glob_list(path_pattern):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            path_list = []
            for path in file_dict:
                if fnmatch.fnmatchcase(path, path_pattern):
                    path_list.append(path)
            return path_list
        return mock_glob_list

    def test_get_exists(self):
        # given
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2 = "local_path/2/*.gz"

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        # when
        build_context = {}

        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.GlobLocalFileSystemTarget(
                glob2, glob2, build_context)

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
            exists1 = glob_target1.get_exists()
            exists2 = glob_target2.get_exists()

        self.assertTrue(exists1)
        self.assertFalse(exists2)

    def test_get_mtime(self):
        # given
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2 = "local_path/2/*.gz"

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        # when
        build_context = {}
        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.GlobLocalFileSystemTarget(
                glob2, glob2, build_context)

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
            mtime1 = glob_target1.get_mtime()
            mtime2 = glob_target2.get_mtime()

        self.assertEqual(mtime1, 2)
        self.assertIsNone(mtime2)

    def test_get_bulk_exists_mtime(self):
        # given
        glob1_pattern = "local_path/1/*.gz"
        glob1 = builder.targets.GlobLocalFileSystemTarget(glob1_pattern,
                                                          glob1_pattern, {})
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2_pattern = "local_path/2/*.gz"
        glob2 = builder.targets.GlobLocalFileSystemTarget(glob2_pattern,
                                                          glob2_pattern, {})

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        build_context = {}

        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1_pattern, glob1_pattern, build_context)

        mtime_fetcher = glob_target1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
                mtimes_exists = mtime_fetcher([glob1, glob2])
                file1_exists = mtimes_exists[glob1_pattern]["exists"]
                file2_exists = mtimes_exists[glob2_pattern]["exists"]
                file1_mtime = mtimes_exists[glob1_pattern]["mtime"]
                file2_mtime = mtimes_exists[glob2_pattern]["mtime"]

        self.assertTrue(file1_exists)
        self.assertFalse(file2_exists)
        self.assertEqual(file1_mtime, 2)
        self.assertEqual(file2_mtime, None)


