# encoding: utf-8

from setuptools import setup

try:
    import pypandoc
    description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    description = ''

setup(name='dwdweather',
      version='0.4',
      description='Inofficial DWD weather data client (Deutscher Wetterdienst)',
      long_description=description,
      author='Marian Steinbach',
      author_email='marian@sendung.de',
      url='http://github.com/marians/dwd-weather',
      py_modules=['dwdweather'],
      install_requires=[],
      entry_points={
          'console_scripts': [
              'dwdweather = dwdweather:main'
          ]
      }
)
