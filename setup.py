from setuptools import setup, find_packages
import os

setup(name='sumo-data-access',
      version='0.0.1',
      description='Scripts for SUMO data access',
      python_requires='>=3.4',
      author='Nancy Wong',
      author_email='nawong@mozilla.com',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      scripts=[s for s in setuptools.findall('bin/') if os.path.splitext(s)[1] != '.pyc'],
      install_requires=[
        'requests'
      ]
)
