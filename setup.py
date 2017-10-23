import re
import ast
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('py_db/__init__.py', 'rb') as f:
    rs = _version_re.search(f.read().decode('utf-8')).group(1)
    version = str(ast.literal_eval(rs))

setup(
    name='py_db',
    version=version,
    install_requires=['sqlalchemy>=1.1.13'],
    description='python db util',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    author='lyt',
    url='https://github.com/taogeYT/py_db',
    author_email='liyt@vastio.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=False,
    zip_safe=True,
)
