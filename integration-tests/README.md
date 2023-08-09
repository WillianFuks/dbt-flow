# dbt-flow: Integration Tests

To run the models as you normally would, go to the flow folder and execute dbt with the target "dev" used as reference:

    dbt seed
    dbt run -t dev

Notice that the files "models/staging/stg__customers.sql" are dependent on source inputs. As they don't exist, simply
comment out the sourcing query and leave only the rest remain in the final select.

To run the tests themselves, the task and target should be changed to 'test':

    dbt test -t test -s tag:flow-test

Each adapter may have their open folders for testing. For testing the bigquery adapter, just to its folder (`./bigquery`) and run:

    dbt test -t test_bigquery -s tag:flow-test
