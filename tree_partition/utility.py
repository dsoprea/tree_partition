import contextlib
import os
import shutil
import tempfile
import json
import logging

import logging.handlers

_LOG_FORMAT = '[%(asctime)s %(name)-20.20s %(levelname)7.7s] %(message)s'

_LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def temp_path():

# TODO(dustin): Add test

    original_wd = os.getcwd()

    path = tempfile.mkdtemp()
    os.chdir(path)

    try:
        yield path
    finally:
        os.chdir(original_wd)

        try:
            shutil.rmtree(path)
        except:
            pass


@contextlib.contextmanager
def chdir(path):

# TODO(dustin): Add test

    original_wd = os.getcwd()
    os.chdir(path)

    try:
        yield
    finally:
        os.chdir(original_wd)


def get_pretty_json(data):
    """Return prettified JSON. Often used with creating storable keys for the
    DB in order to both convert structures to strings *and* ensure that they're
    deterministically (reproducibly) ordered (alphabetical, here).
    """

    return \
        json.dumps(
            data,
            sort_keys=True,
            indent=4,
            separators=(',', ': '))


def configure_logging(args):
    logger = logging.getLogger()

    if args.is_verbose is True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.ERROR)

    if args.log_filepath is not None:
        # More oftenthen not, we'll rerun the same command, forget to remove or
        # change the name of the file, and then have to stop, do it, and
        # restart. Not doing an append, here, is an optimization since append
        # is only rarely beneficial.
        handler = logging.FileHandler(args.log_filepath, mode='w')

        formatter = logging.Formatter(_LOG_FORMAT)
        handler.setFormatter(formatter)

# TODO(dustin): Needs to be performance tested.
        # If we have a ton of logging, this will help make the writing
        # more asynchronous in order to not slow down the process.
        if args.use_buffered_logs is True:
            handler = logging.handlers.MemoryHandler(1000, target=handler)

        logger.addHandler(handler)
    else:
        logging.basicConfig(format=_LOG_FORMAT)


def register_common_parameters(parser):
    parser.add_argument(
        '-l', '--log-filepath',
        help="Redirect logging to a file")

    parser.add_argument(
        '--buffered-logs',
        dest='use_buffered_logs',
        action='store_true',
        help="Buffer logs in order to mitigate effects of volume logging")

    parser.add_argument(
        '-v', '--verbose',
        dest='is_verbose',
        action='store_true',
        help="Show debug logging")


def deterministically_enumerate_tree(root_path):

# TODO(dustin): Add test

    prefix_len = len(root_path) + 1

    for path, folders, files in os.walk(root_path):

        # Ensure that the visit order is deterministic. Supports testing and
        # reproducible results for large datasets in the real-world at the the
        # [manageable, usually] expense of some efficiency.
        folders.sort()

        for filename in files:
            filepath = os.path.join(path, filename)
            rel_filepath = filepath[prefix_len:]

            yield rel_filepath


def partition_path_by_mod_gen(root_path, n, predicate=None):
    """Enumerate all files and yield each with a partition number in a
    deterministic order. This will not yield directories.
    """

    files = deterministically_enumerate_tree(root_path)

    i = 0
    for rel_filepath in files:

        if predicate is not None and predicate(rel_filepath) is False:
            continue

        yield i % n, rel_filepath

        i += 1


def partition_and_link(
        source_root_path, n, target_root_path, labels=None, predicate=None,
        relative_prefix=None):

    source_root_path = source_root_path.rstrip(os.sep)
    target_root_path = target_root_path.rstrip(os.sep)

    assert \
        labels is None or len(labels) == n, \
        "`labels` must None or have a length that matches N: " \
            "{} != ({})".format(labels, n)


    # Unify handling when no labels given
    if labels is None:
        labels = [
            str(i)
            for i
            in range(n)
        ]


    # Link files into the partition paths

    tagged_files = \
        partition_path_by_mod_gen(
            source_root_path,
            n,
            predicate=predicate)

    counts = {}
    for i, (partition_number, rel_filepath) \
            in enumerate(tagged_files):

        partition = labels[partition_number]

        target_filepath = \
            os.path.join(
                target_root_path,
                partition,
                rel_filepath)


        # Make sure the target path exists

        target_path = os.path.dirname(target_filepath)
        if os.path.exists(target_path) is False:
            os.makedirs(target_path)


        if relative_prefix is not None:
            link_target = \
                os.path.join(
                    relative_prefix,
                    rel_filepath)

        else:
            link_target = \
                os.path.join(
                    source_root_path,
                    rel_filepath)


        try:
            os.symlink(link_target, target_filepath)

        except FileExistsError:

            # We might inevitably need to support an existing path, especially
            # with usch large numbers as to require us in the first place

            pass


        try:
            counts[partition] += 1
        except KeyError:
            counts[partition] = 1


        if (i + 1) % 1000 == 0:
            _LOGGER.debug("({}) matching files processed.".format(i + 1))


    return counts
