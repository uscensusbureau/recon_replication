from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='recon_replication',
    version='1.0',
    description='A replication package that reconstructs microdata from a subset of 2010 Census Summary File 1 table',
    url='https://github.com/uscensusbureau/recon_replication',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        'Programming Language :: Python :: 3.8',
    ],
)
