from setuptools import setup, find_packages, Command
import subprocess

class YumLocalMysql(Command):
    """A Yum Installation of Mysql Server"""

    description = 'A Yum Installation of Mysql Server'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # Run some additional setup steps here
        subprocess.run(['sudo', 'yum', 'install', 'mysql-server*8.0.28*'])
        subprocess.run(['sudo', 'yum', 'install', 'mysql*8.0.28*'])



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
    cmdclass={
        'YumLocalMysql': YumLocalMysql,
    }
)
