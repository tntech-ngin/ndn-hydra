# -------------------------------------------------------------
# NDN Hydra Pip Setup
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import io
import re
from setuptools import setup, find_packages
from typing import List

with io.open("./docs/version.py", "rt", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

with io.open("README.rst", "rt", encoding="utf8") as f:
    long_description = f.read()


def _parse_requirements(filename: str) -> List[str]:
    with open(filename, 'r') as f:
        return [s for s in [line.split('#', 1)[0].strip(' \t\n') for line in f] if s != '']


setup(
    name='ndn-hydra-repo',
    version=version,
    description='ndn-hydra: An NDN distributed repository with resiliency coded in python.',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/tntech-ngin/ndn-hydra',
    author='See GitHub',
    author_email='sshannigrahi@tntech.edu',
    maintainer='see GitHub',
    maintainer_email='sshannigrahi@tntech.edu',
    download_url='https://pypi.python.org/pypi/ndn-hydra',
    project_urls={
        "Bug Tracker": "https://github.com/tntech-ngin/ndn-hydra/issues",
        "Source Code": "https://github.com/tntech-ngin/ndn-hydra",
    },
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Software Development :: Libraries',
        'Topic :: Database',
        'Topic :: Internet',
        'Topic :: System :: Networking',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    keywords='NDN HYDRA',
    packages=find_packages(exclude=['tests', 'examples', 'notes']),
    include_package_data=True,
    package_data={'ndn_hydra.repo': ['config.yaml', 'docs/version.py', 'docs/requirements.txt']},
    install_requires=_parse_requirements('docs/requirements.txt'),
    python_requires=">=3.8",
    entry_points={
        'console_scripts': [
            'ndn-hydra-repo = ndn_hydra.repo.main.main:main',
            'ndn-hydra-client = ndn_hydra.client.main:main'
        ]
    },
    zip_safe=False
)
