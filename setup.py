from setuptools import setup, find_packages
import pathlib
here = pathlib.Path(__file__).parent
txt = (here / 'README.rst').read_text()

setup(
    name="distask",
    version="0.1.2",
    keywords="distribute scheduling cron",
    description="a distribute task scheduler ",
    long_description=txt,
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
        'redlock',
        'pymongo'
    ],
    extras_require={
        'extra': ['dill']
    }

)