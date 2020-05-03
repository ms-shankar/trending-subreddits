#!/usr/bin/env python
# coding=utf-8

"""
python distribute file
"""

from setuptools import setup, find_packages

with open('./requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="trending-subreddits",
    version="1.0.0",
    author="M S Shankar",
    author_email="m.s.shankar13689@gmail.com",
    description="Ranking of Trending Subreddits",
    python_requires='>=3.7',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3.7'
    ]
)