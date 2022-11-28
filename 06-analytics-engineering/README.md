# Analytics Engineering

Create a dbt project

```sh
dbt init
```

Edit the dbt profiles

```sh
code ~/.dbt/profiles.yml
```

```yml
jaffle:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: public

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: postgres
      schema: prod

  target: dev
```

Test dbt connection

```sh
cd jaffle
dbt debug
```

You should see "All checks passed!".

To create models

```sh
dbt run
```

To test models

```sh
dbt test
```

To view docs (on Gitpod)

```sh
dbt docs generate
dbt docs serve --no-browser

```

## Lineage graph
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/06-analytics-engineering/1.jpg" height="700" width="1000" >