# Jaffle Shop Unit Testing

To run tests against the Jaffle Shop example project, just run the following in the jaffle_shop folder:

    dbt test -t postgres

To select a specific test:

    dbt test -t postgres -s tag:unit-test
