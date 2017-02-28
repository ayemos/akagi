#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'boto3>=1.4.4',
    'numpy>=1.12',
    'tqdm>=4.11'
]

test_requirements = [
    'moto>=0.4'
]

setup(
    name='osho',
    version='0.1.0',
    description="Machine Learning test framework.",
    long_description=readme + '\n\n' + history,
    author="Yuichiro Someya",
    author_email='ayemos.y@gmail.com',
    url='https://github.com/ayemos/osho',
    packages=[
        'osho',
        'osho.datasets',
        'osho.evaluators',
        'osho.evaluators.extensions',
        'osho.models',
        'osho.utils'
    ],
    package_dir={'osho':
                 'osho'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='osho',
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
