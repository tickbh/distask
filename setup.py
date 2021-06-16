from setuptools import setup, find_packages

setup(
    name="distask",
    version="0.1",
    keywords="distribute scheduling cron",
    description="a distribute task scheduler ",
    long_description="file: README.rst",
    license="MIT Licence",
    author="tickbh",
    author_email="tickdream125@hotmail.com",
    # packages=find_packages("distask"),
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
)