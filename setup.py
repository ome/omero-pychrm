#!/usr/bin/env python

"""
setup.py file for OmeroPychrm
"""

from setuptools import setup

setup(
    name='OmeroPychrm',
    version='0.1.0',
    author='Simon Li',
    author_email='spli@dundee.ac.uk',
    packages=['OmeroPychrm'],
    url='http://www.openmicroscopy.org/',
    license='LICENSE.txt',
    description='Scripts for using PyCHRM in OMERO',
    long_description=open('README.md').read(),
    install_requires=[
        'pychrm>=0.1.0',
        ],
    dependency_links=['svn+http://wnd-charm.googlecode.com/svn/pychrm/trunk/@675#egg=pychrm-0.1.0'],
)



