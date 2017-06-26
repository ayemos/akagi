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

akagi supports *iter* and *save* interface for various data sources such as Amazon Redshift, Amazon S3 (more in future).

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
Example
--------

++++++++++++++++++
MySQLDataSource
++++++++++++++++++

.. code:: python

  with MySQLDataSource.for_query(
          'select * from (select user_id, path from logs.imp limit 10000)', # Your Query here
          ) as ds:
      ds.save('./akagi_test') # save results to local

      for d in ds:
          print(d) # iterate on result

++++++++++++++++++
RedshiftDataSource
++++++++++++++++++

.. code:: python

  with RedshiftDataSource.for_query(
          'select * from (select user_id, path from logs.imp limit 10000)', # Your Query here
          'logs', # schema
          'imp', # table (Those two are used to generate unique prefix for S3 object (e.g. logs/imp/20170312_081527)
          'log-redshift-unload.ap-northeast-1', # S3 Bucket for intermediate storage
          ) as ds:
      ds.save('./akagi_test') # save results to local

      for d in ds:
          print(d) # iterate on result

++++++++++++
S3DataSource
++++++++++++


.. code:: python

  with S3DataSource.for_prefix(
          'image-data.ap-northeast-1',
          'data/image_net/zebra',
          FileFormat.BINARY) as ds:
      ...

++++++++++++++++++
LocalDataSource
++++++++++++++++++

.. code:: python

  with LocalDataSource.for_path(
        './PATH/TO/YOUR/DATA/DIR',
        'csv') as ds:
      ds.save('./akagi_test') # save results to local

      for d in ds:
          print(d) # iterate on result

--------
Credits
--------

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_ and the
`audreyr/cookiecutter-pypackage <https://github.com/audreyr/cookiecutter-pypackage>`_ project template.
