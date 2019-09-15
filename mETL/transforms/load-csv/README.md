# Postgres

Load to postgres by specifying a configuration file that resembles:

```
{
    "connection": "dbname=dcss"
}
```

Note that the `connection` value is anything that can be used with [`psycpg2`'s `connect` method](http://initd.org/psycopg/docs/module.html).

# sqlite

```
{
    "database": "~/path/to/database.sqlite"
}
```