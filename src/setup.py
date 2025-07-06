"""
Setup configuration for the bankvboa application

Written by Daniel Brosnan Blázquez

module bankvboa
"""
from setuptools import setup, find_packages

setup(name="bankvboa",
      version="1.0.0",
      description="Engine and visualization tool for bank account analysis",
      author="Daniel Brosnan",
      author_email="daniel.brosnan@deimos-space.com",
      packages=find_packages(),
      include_package_data=True,
      python_requires='>=3',
      install_requires=[
          "eboa",
          "vboa",
          "massedit",
          "xlrd",
          "pandas"
      ],
      extras_require={
          "tests" :[
              "nose",
              "before_after",
              "coverage",
              "termcolor",
              "pytest-cov",
              "Sphinx",
              "selenium==3.14"
          ]
      },
      test_suite='nose.collector')
