import os
import setuptools

with open(os.path.join('tree_partition', 'resources', 'README.md')) as f:
    long_description = f.read()

with open(os.path.join('tree_partition', 'resources', 'requirements.txt')) as f:
    install_requires = list(map(lambda s: s.strip(), f.readlines()))

_VERSION = '0.0.1'

setuptools.setup(
    name='tree_partition',
    version=_VERSION,
    description="Split filesystem tree into equal parts.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[],
    author='Dustin Oprea',
    author_email='dustin@randomingenuity.com',
    packages=setuptools.find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    package_data={
        'tree_partition': [
            'resources/README.md',
            'resources/requirements.txt',
            'resources/requirements-testing.txt',
        ],
    },
    install_requires=install_requires,
    scripts=[
    ],
)
