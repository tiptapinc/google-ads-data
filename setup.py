from setuptools import setup

setup(
    name='google_ads_data',
    version=open('VERSION').read().strip(),
    author='MotiveMetrics',
    install_requires=[
        'boto3',
        'google-ads',
        'pandas',
        'PyMongo',
        'PyYAML'
    ],
    packages=['google_ads_data']
)
