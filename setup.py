import setuptools
from setuptools import setup
import os

with open('requirements.txt') as f:
    required = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='parsons-utilities',
    version='0.0.8',
    author='Emily Cogsdill',
    author_email='emily.cogsdill@techallies.org',
    description='Parsons modules for Airbyte deployment',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/community-tech-alliance/parsons-utilities',
    project_urls = {
    },
    license='MIT',
    packages=['parsons_utilities'],
    install_requires=required
)