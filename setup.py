from setuptools import setup, find_packages

setup(
    name='iconicretail',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'pyspark',
    ],
    python_requires='>=3.10',
)
