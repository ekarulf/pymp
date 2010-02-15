from setuptools import setup, find_packages

setup(
    name = "pymp",
    version = "0.1",
    url = 'http://www.fort-awesome.net/wiki/pymp',
    license = 'MIT',
    description = "A very specific case when Python's multiprocessing library doesn't work",
    author = 'Erik Karulf',
    # Below this line is tasty Kool-Aide provided by the Cargo Cult
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    install_requires = ['setuptools'],
)