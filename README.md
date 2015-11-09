# etl-framework

[![Circle CI](https://circleci.com/gh/pantheon-systems/etl-framework.svg?style=svg)](https://circleci.com/gh/pantheon-systems/etl-framework)


How to use it
=============
```
SQL_DSN = 'mysql://$USER:$PASSWORD@HOST:$PORT/$DATABASE_NAME'

from etl_framework.BaseMySqlLoader import BaseMySqlLoader
sql_loader = BaseMySqlLoader()
sql_loader.create_from_dsn(SQL_DSN)
STATEMENT = 'SELECT * FROM _sites_ LIMIT 1'
data, field_names = sql_loader.run_statement(STATEMENT, fetch_data=True)
```

Tables from Cassandra
=====================
BI Table name  | Created from
------------- | -------------
`_users_` | `users, profiles_by_user, user_tracking`
`_sites_` | `sites, attributes_by_site`
`_organizations_` | `organizations`
`_instruments_` | `instruments`
`_organization_site_keys_` | `memberships` tables
`_organization_user_keys_` | `memberships` tables
`_site_user_keys_` | `memberships` tables
`_hostnames_` | `hostnames, hostnames_v2`
`_workflows_*` | `workflows`
