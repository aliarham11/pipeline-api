import setuptools

REQUIRED_PACKAGES = [
    'jsonschema==3.0.0',
]

setuptools.setup(
    name='data-lake-sink',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
 )