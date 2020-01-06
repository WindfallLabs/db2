#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

# NOTES:
# coverage run --source db2 -m py.test; coverage html
#

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', 'coverage']

setup(
    author="Garin Wally",
    author_email='garwall101@gmail.com',
    python_requires='>=2.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7'
        #'Programming Language :: Python :: 3',
        #'Programming Language :: Python :: 3.5',
        #'Programming Language :: Python :: 3.6',
        #'Programming Language :: Python :: 3.7',
        #'Programming Language :: Python :: 3.8',
    ],
    description="Python Boilerplate contains all the boilerplate you need to create a Python package.",
    entry_points={
        'console_scripts': [
            'db2=db2.cli:main',
        ],
    },
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='db2',
    name='db2',
    packages=find_packages(include=['db2', 'db2.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/WindfallLabs/db2',
    version='0.0.1',
    zip_safe=False,
)
