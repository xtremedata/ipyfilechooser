#!/usr/bin/env python
import os
from typing import AnyStr
from setuptools import setup, find_packages


def read(fname: str) -> str:
    """Open files relative to package."""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='ipyfilechooser',
    version='0.6.1',
    author='Thomas Bouve (@crahan)',
    author_email='crahan@n00.be',
    description=(
        'Python file chooser widget for use in '
        'Jupyter/IPython in conjunction with ipywidgets'
    ),
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    url='https://github.com/crahan/ipyfilechooser',
    license='MIT',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
    ],
    install_requires=[
        'ipywidgets'
    ]
)
