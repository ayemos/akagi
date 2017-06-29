#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'boto3>=1.4.4',
    'psycopg2>=2.7',
    'mysqlclient>=1.3.10'
]

test_requirements = [
    'codeclimate-test-reporter>=0.1.2'
]

setup(
    name='akagi',
    version='0.1.14',
    description="Codenize your data sources",
    long_description=readme,
    author="Yuichiro Someya",
    author_email='ayemos.y@gmail.com',
    url='https://github.com/ayemos/akagi',
    packages=[
        'akagi',
        'akagi.data_sources',
        'akagi.data_files',
        'akagi.data_file_bundles',
        'akagi.iterators'
    ],
    package_dir={'akagi':
                 'akagi'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='akagi',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
