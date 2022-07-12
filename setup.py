from setuptools import setup, find_packages

setup(
    name='google-ads-data',
    version=open('VERSION').read().strip(),
    author='MotiveMetrics',
    install_requires=[
        'boto3',
        'google-ads',
        'pandas',
        'PyMongo',
        'PyYAML'
    ],
    packages=find_packages(exclude=["tests"])
)
