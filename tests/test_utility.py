import unittest
import os

import tree_partition.utility


class TestUtility(unittest.TestCase):
    def _touch_file(self, filepath):
        path = os.path.dirname(filepath)

        if path != '' and os.path.exists(path) is False:
            os.makedirs(path)

        with open(filepath, 'w') as f:
            f.write('content:')
            f.write(filepath)

    def test_partition_path_by_mod_gen(self):
        with tree_partition.utility.temp_path() as path:

            self._touch_file('file1')
            self._touch_file('aa/file2')
            self._touch_file('aa/bb/file3')
            self._touch_file('aa/bb/file4')
            self._touch_file('aa/bb/cc/file5')
            self._touch_file('aa/bb/cc/file6')
            self._touch_file('aa/bb/dd/file7')
            self._touch_file('ee/file8')
            self._touch_file('ee/ff/file9')
            self._touch_file('ee/gg/file10')

            partitioned = tree_partition.utility.partition_path_by_mod_gen(path, 3)
            partitioned = list(partitioned)

            expected = [
                (0, 'file1'),
                (1, 'aa/file2'),
                (2, 'aa/bb/file4'),
                (0, 'aa/bb/file3'),
                (1, 'aa/bb/cc/file6'),
                (2, 'aa/bb/cc/file5'),
                (0, 'aa/bb/dd/file7'),
                (1, 'ee/file8'),
                (2, 'ee/ff/file9'),
                (0, 'ee/gg/file10'),
            ]

            self.assertEquals(partitioned, expected)

    def test_partition_and_link(self):

        with tree_partition.utility.temp_path() as path:


            # Create and fill source path

            source_path = os.path.join(path, 'source')
            os.mkdir(source_path)

            with tree_partition.utility.chdir(source_path):
                self._touch_file('file1')
                self._touch_file('aa/file2')
                self._touch_file('aa/bb/file3')
                self._touch_file('aa/bb/file4')
                self._touch_file('aa/bb/cc/file5')
                self._touch_file('aa/bb/cc/file6')
                self._touch_file('aa/bb/dd/file7')
                self._touch_file('ee/file8')
                self._touch_file('ee/ff/file9')
                self._touch_file('ee/gg/file10')


            # Partition

            target_path = os.path.join(path, 'target')

            n = 3

            counts = \
                tree_partition.utility.partition_and_link(
                    source_path,
                    n,
                    target_path,
# TODO(dustin): !! Add test for `labels_index`
                    labels=None)

            expected = {
                '0': 4,
                '1': 3,
                '2': 3,
            }

            self.assertEquals(counts, expected)


            # Read target path

            target_rel_filepaths = \
                tree_partition.utility.deterministically_enumerate_tree(
                    target_path)

            prefix_len = len(path) + 1

            link_index = {}
            contents_index = {}
            for target_rel_filepath in target_rel_filepaths:
                target_filepath = os.path.join(target_path, target_rel_filepath)
                link_filepath = os.readlink(target_filepath)
                rel_link_filepath = link_filepath[prefix_len:]

                link_index[target_rel_filepath] = rel_link_filepath

                with open(link_filepath) as f:
                    contents_index[rel_link_filepath] = f.read()


            # Check the existence of the symlinks and who they point to

            expected = {
                '0/aa/bb/dd/file7': 'source/aa/bb/dd/file7',
                '0/aa/bb/file3': 'source/aa/bb/file3',
                '0/ee/gg/file10': 'source/ee/gg/file10',
                '0/file1': 'source/file1',
                '1/aa/bb/cc/file6': 'source/aa/bb/cc/file6',
                '1/aa/file2': 'source/aa/file2',
                '1/ee/file8': 'source/ee/file8',
                '2/aa/bb/cc/file5': 'source/aa/bb/cc/file5',
                '2/aa/bb/file4': 'source/aa/bb/file4',
                '2/ee/ff/file9': 'source/ee/ff/file9'
            }

            self.assertEquals(link_index, expected)


            # Check contents of the files pointed-to by links

            expected = {
                'source/aa/bb/cc/file5': 'content:aa/bb/cc/file5',
                'source/aa/bb/cc/file6': 'content:aa/bb/cc/file6',
                'source/aa/bb/dd/file7': 'content:aa/bb/dd/file7',
                'source/aa/bb/file3': 'content:aa/bb/file3',
                'source/aa/bb/file4': 'content:aa/bb/file4',
                'source/aa/file2': 'content:aa/file2',
                'source/ee/ff/file9': 'content:ee/ff/file9',
                'source/ee/file8': 'content:ee/file8',
                'source/ee/gg/file10': 'content:ee/gg/file10',
                'source/file1': 'content:file1',
            }

            self.assertEquals(contents_index, expected)
