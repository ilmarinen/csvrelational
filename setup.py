from setuptools import setup, find_packages
from os import path


setup(
    name='csvrelational',
    version='0.0.1',
    description='A way to transform related CSV files to SQLAlchemy models',
    url='https://github.com/ilmarinen/csvrelational.git',
    author='Praphat Xavier Fernandes',
    author_email='fernandes.praphat@gmail.com',
    license='BSD',
    classifiers=[
        'Development Status :: Alpha',
        'Programming Language :: Python 2.7'
    ],

    packages=find_packages(exclude=['tests*']),
    keywords='csv database sqlalchemy relational',
    install_requires=['pandas', 'SQLAlchemy']
)
