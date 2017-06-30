from akagi.data_sources import MySQLDataSource

with MySQLDataSource.for_query(
        'select * from soup_predictions limit 100',
        {
            'host': 'cookpad-all-staging.cyaxvia5gf7a.ap-northeast-1.rds.amazonaws.com',
            'db': 'search_summary',
            'user': 'ckpd_readonly',
            'password': 'no3lnar;'
            }
        ) as ds:
    for d in ds:
        print(d)
