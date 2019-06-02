# encoding: utf-8

from setuptools import setup

try:
    import pypandoc
    description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    description = ''

setup(name='dwdweather2',
      version='0.8.0dev1',
      description='Inofficial DWD weather data client (Deutscher Wetterdienst)',
      long_description=description,
      author='Marian Steinbach',
      author_email='marian@sendung.de',
      url='https://github.com/hiveeyes/dwdweather2',
      py_modules=['dwdweather'],
      install_requires=[
          'tqdm==4.32.1',
          'python-dateutil==2.8.0',
      ],
      entry_points={
          'console_scripts': [
              'dwdweather = dwdweather:main'
          ]
      }
)
