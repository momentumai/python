from os import path


def read_file(filename, relative_path):
    return open(
        path.dirname(path.realpath(filename)) + relative_path,
        'r'
    ).read()
