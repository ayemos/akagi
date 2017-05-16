from akagi.data_sources import MySQLDataSource

with MySQLDataSource.for_query(
        "select * from recipes limit 10;",
        db_conf={'db': 'cookpad_data'}) as ds:
    for d in ds:
        print(d)

