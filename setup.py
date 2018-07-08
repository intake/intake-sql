#!/usr/bin/env python

from setuptools import setup
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-sql',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='SQL plugin for Intake',
    url='https://github.com/ContinuumIO/intake-sql',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_sql'],
    packages=['intake_sql'],
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
