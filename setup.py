# This file is part of PyArweave.
# 
# PyArweave is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 2 of the License, or (at your option) any later
# version.
# 
# PyArweave is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with
# PyArweave. If not, see <https://www.gnu.org/licenses/>.

from setuptools import setup, find_packages

setup(
  name='libvastai',
  version='0.0.2-1',
  description='Unofficial Vast.AI Library',
  long_description=open('README.md').read(),
  long_description_content_type='text/markdown',
  url='https://github.com/xloem/libvastai',
  keywords=[],
  classifiers=[
    'Programming Language :: Python :: 3',
    'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
    'Operating System :: OS Independent',
  ],
  packages = find_packages(),
  package_data = {
    'vast': ['vast_python/*.py']
  },
  install_requires=[
    'requests', # used by vast_python to connect to the api server
    'borb' # vast_python imports this for generating pdfs, not presently used
  ],
)
