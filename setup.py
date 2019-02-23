# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='oceanproteinportal-py',
    version='0.1.0',
    description='Python module for the Ocean Protein Portal https://www.oceanproteinportal.org',
    long_description=readme,
    author='Adam Shepherd',
    author_email='webmaster@oceanproteinportal.org',
    url='https://github.com/oceanproteinportal/oceanproteinportal-py',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
