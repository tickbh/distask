from setuptools import setup, find_packages
from os import path
here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="distask",
    version="0.1.10",
    keywords="distribute scheduling cron",
    description="a distribute task scheduler ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT Licence",
    author="tickbh",
    author_email="tickdream125@hotmail.com",
    packages=[
        'distask', 
        'distask/locks', 
        'distask/datastores', 
        'distask/schedulers', 
        'distask/serializers', 
        'distask/tiggers', 
        'distask/tiggers/cron'
    ],
    # py_modules=['distask/*', 'distask/locks/*'],
    platforms="any",
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=[
        'tzlocal',
        'pytz',
        'kazoo',
        'redlock',
        'pymongo'
    ],
    extras_require={
        'extra': ['']
    }

)