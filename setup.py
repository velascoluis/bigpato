# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "bigpato"
LONG_DESCRIPTION = "bigpato package"

# Setting up
setup(
    name="bigpato",
    version=VERSION,
    author="Luis Velasco",
    author_email="<luis.velasco@gmail.com>",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    package_dir={"bigpatopkg": "bigpato"},
    include_package_data=True,
    install_requires=[
        "duckdb",
        "sqlglot",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pandas",
        "db-dtypes"
    ],
    keywords=["python", "bigpato"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X"
    ],
)