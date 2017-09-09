==========
akagi
==========

.. image:: https://img.shields.io/pypi/v/akagi.svg
  :target: https://pypi.python.org/pypi/akagi

.. image:: https://img.shields.io/travis/ayemos/akagi.svg
  :target: https://travis-ci.org/ayemos/akagi

.. image:: https://readthedocs.org/projects/akagi/badge/?version=latest
  :target: https://akagi.readthedocs.io/en/latest/?badge=latest

.. image:: https://pyup.io/repos/github/ayemos/akagi/shield.svg
  :target: https://pyup.io/repos/github/ayemos/akagi/

.. image:: https://codeclimate.com/github/ayemos/akagi/badges/coverage.svg
  :target: https://codeclimate.com/github/ayemos/akagi/coverage

###########
akagi
###########

* Free software: MIT license

---------
Features
---------

akagi enables you to access various data sources such as Amazon Redshift, Amazon S3 and Google Spreadsheet (more in future) from python.

-------------
Installation
-------------

Install via pip::

  pip install akagi

or from source::

  $ git clone https://github.com/ayemos/akagi akagi
  $ cd akagi
  $ python setup.py install


--------
Setup
--------

To use RedshiftDataSource, you need to set environment variable `AKAGI_UNLOAD_BUCKET` the name
of the Amazon S3 bucket you like to use as intermediate storage of Redshift Unload command.


::

  $ export AKAGI_UNLOAD_BUCKET=xyz-unload-bucket.ap-northeast-1


To use SpreadsheetDetaSource, you need to set environment variable `GOOGLE_APPLICATION_CREDENTIAL` to
indicate your service account credentials file. You can get the credential from `here <https://console.developers.google.com/permissions/serviceaccounts>`_.

Associated client has to have read access to the sheets.


::

  $ export GOOGLE_APPLICATION_CREDENTIAL=$HOME/.credentials/service-1a2b.json

--------
Example
--------

++++++++++++++++++
RedshiftDataSource
++++++++++++++++++

.. code:: python

  from akagi.data_sources import RedshiftDataSource

  ds = RedshiftDataSource('select * from (select user_id, path from logs.imp limit 10000')

  for d in ds:
      print(d) # iterate on result

++++++++++++
S3DataSource
++++++++++++


.. code:: python

  from akagi.data_sources import S3DataSource

  ds = S3DataSource.for_prefix(
          'image-data.ap-northeast-1',
          'data/image_net/zebra',
          file_format='binary')

  for d in ds:
      print(d) # iterate on result

+++++++++++++++++++++
SpreadsheetDataSource
+++++++++++++++++++++

.. code:: python

  from akagi.data_sources import LocalDataSource

  ds = SpreadsheetDataSource(
        '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',  # sample sheet provided by Google
        sheet_range='Class Data!A2:F31')

  for d in ds:
      print(d) # iterate on result

++++++++++++++++++
LocalDataSource
++++++++++++++++++

.. code:: python

  from akagi.data_sources import LocalDataSource

  ds = LocalDataSource(
        './PATH/TO/YOUR/DATA/DIR',
        file_format='csv')

  for d in ds:
      print(d) # iterate on result

--------
Credits
--------

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_ and the
`audreyr/cookiecutter-pypackage <https://github.com/audreyr/cookiecutter-pypackage>`_ project template.
