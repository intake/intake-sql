#!/usr/bin/env python

from setuptools import setup
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-odbc',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='ODBC plugin for Intake',
    url='https://github.com/ContinuumIO/intake-odbc',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    py_modules=['intake_odbc'],
    packages=['intake_odbc'],
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
