#/usr/bin/env python
import os
from setuptools import setup, find_packages


ROOT_DIR = os.path.dirname(__file__)
SOURCE_DIR = os.path.join(ROOT_DIR)


setup(
    name="django-longerusernameandemail",
    version="0.5.6",
    packages=find_packages(),
    zip_safe=False,
    description=("django-longerusernameandemail provides a migration and a "
                 "monkeypatch to make the django auth.user username and email "
                 "fields longer, instead of the arbitrarily short 30 and 75 "
                 "characters."),
    license="BSD",
    keywords="django long email username migration south",
)
