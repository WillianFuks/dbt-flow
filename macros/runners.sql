{#/*
  setup_test(model_name, test_name, expected, mocks={}, options={}, test_description=None)

  Helper function working as a friendly API for end users to build the setup
  of their tests.

  Parameters
  ----------
  target_model: str
              Name of the node being tested. For instance, if in marts folders there's
              a model for "orders" entity, to test this node simply create a test where
              target_model="orders".
  test_name: str
             Identifies this test being run. This value is also used to create the mocked
             tables in the database.
  expected: str
            Query that sets what should be observed as final result from all mocks being
            processed through the flow of data transformations. For instance, if "orders"
            is being tested and all its inputs have been mocked already, then this value
            will dictate whether the test was a success or not by comparing the result
            with the expected value:

            expected="""
              select 1 as id, 100 as value
            """

            If the "orders" tables created in the database have the same data as the
            expected query then the test passes. It raises an error if there's any
            difference.
  mocks: Dict[str, str]
         Identifies which nodes will be mocked and the queries to replace their data
         content. Keys are the nodes to be mocked, values the queries that will mock
         the data. For instance:

         {
            "orders": """
              select 1 as id, 1 as user_id, 100 as value union all
              select 2 as id, 1 as user_id, 102 as value
            """
          }

          This indicates to dbt_flow that the node "orders" must be replaced with the
          query as given by the key value. All mocks should be placed in this key. In
          order to mock sources, simply use the source name ~ \dot (".") ~ source table,
          for example:

          {
            "source_name.source_table" : """
              select 1 as id union all
              select 2
            """
          }

          And dbt_flow will scan through the flattened graph looking for this key. Seeds
          should also be mocked here (if so desired by the end user) as a query --adding
          support to read CSVs in Jinja itself was not implemented yet, primarily because
          we don't have access to `agate_helper` module in our runtime environment.
  options: Dict[str, Any]
           Settings that sets how this test should run.
  options.test_incremental: bool
                            If `true` then all nodes that are incremental will be created
                            as incremental tables when mocked. The steps are to first create
                            the mocked table as incremental and then proceed to build all
                            dependency nodes and then run again the query for the node which
                            will this time trigger the `is_incremental()` functionality.
  options.verbose: bool
                   If `true` then prints additional information to stdout. Defaults to `false`.
  test_description: Optional[str]

  Raises
  ------
    If non-optional input parameters are `None`.
*/#}
{%- macro setup_test(target_model, test_name, expected, mocks={}, options={}, test_description=None) -%}

  {%- if not target_model or not test_name or not expected -%}
    {%- do dbt_flow.raise_error('Inputs `target_model`, `test_name` and `expected` are mandatory! Please make sure to send all those as input to `setup_test` macro.') -%}
  {%- endif -%}

  {%- do return(
    {
      "target_model": target_model,
      "test_name": test_name,
      "expected": expected,
      "mocks": mocks,
      "options": options,
      "test_description": test_description,
    }
  ) -%}

{%- endmacro -%}


{#/*
  build_node(node_uid, test_setup, cached_nodes)

  This is probably the most important and complex macro in dbt_flow. It's responsible
  for scanning through all the node dependencies from the target node being tested
  and recursively build all nodes and their mocks (if available).
  The contract of `build_node` accepts the unique id of the node instead of its config
  as this makes its code a bit simpler and more direct. This forces the target node to
  be scanned twice but as this task is quite cheap then it remained as is.

  Parameters
  ----------
    node_uid: str
              Unique id of the node being tested, for instance,
              "model.dbt_flow_integration_tests.metrics_customers".

    test_setup: Dict[str, Any]
                Setup of the test as given by the user input.

    cached_nodes: Dict[str, Dict[str, str]]
                  Keys in this variable indicate which nodes were already processed
                  and are given by their unique_id. The values are also another dict
                  where only two values are expected: "original_relation_name" and
                  "mocked_relation". The former indicates the original relation the
                  node had and the latter the newer relation that dbt_flow mocked in the query.
                  As an example:

                  cached_nodes = {
                    "model.dbt_flow_integration_tests.metrics_customers": {
                      "original_relation_name": "postgres"."dbt_dev"."metrics_customers",
                      "mocked_relation": api.Relation
                    }
                  }

                  We do so because for each node being rendered we need to loop through its
                  dependencies nodes, build each of those and then processed to build the
                  current node. But the mocked nodes are built with different relation names
                  and those need to be replaced in the current node query as well.

                  The cached nodes holds this information so we can make all the necessary
                  substitutions and finally build the current node. Without this caching
                  approach we'd need to loop through all dependencies again to find their
                  original relation name and process what the newer mocked relation name is.
*/#}
{%- macro build_node(node_uid, test_setup, cached_nodes) -%}

  {%- do dbt_flow.log(test_setup.test_name, 'Processing node: ' ~ node_uid, test_setup.options.get('verbose', false)) -%}
  {%- set node_config = dbt_flow.get_graph_node_config('unique_id', node_uid) -%}

  {# --If a given source have the exact same name as some other model then we'd get a collision. To avoid that we use the
     -- source's identifier instead and append the string "s_" to indicate the mock is related to a source. #}
  {%- set node_identifier = 's_' ~ node_config.identifier if node_config.resource_type == 'source' else node_config.name -%}

  {%- set mocked_relation = api.Relation.create(database=target.database, schema=target.schema,
    identifier=dbt_flow.build_flow_identifier(test_name=test_setup.test_name, identifier=node_identifier)) -%}

  {#/*
    We delete all nodes before processing them to avoid incremental nodes from being targeted as already existing by mistake from previous
    test runs that didn't drop the relations.
  */#}
  {%- if load_cached_relation(mocked_relation) -%}
    {%- do adapter.drop_relation(mocked_relation.incorporate(type='table')) -%}
  {%- endif -%}

  {#/*
     First look for sources mocks as their node name can have collisions with other models, such as a source with name "orders"
     and a mock for the model "orders" as well. In this case, the mock of "orders" would replace the source mock with the same name.
  */#}
  {%- set mocked_sql = test_setup.mocks.get(node_config.source_name ~ '.' ~ node_config.name) |
    default(test_setup.mocks.get(node_config.name), true) -%}

  {%- if mocked_sql -%}

    {%- do dbt_flow.create_table(relation=mocked_relation, sql=mocked_sql, config=node_config) -%}

    {%- do dbt_flow.log(test_setup.test_name, 'Mocked table "' ~ mocked_relation ~ '" for node "' ~ node_config.name ~ '" created successfully', test_setup.options.get('verbose', false)) -%}

    {%- do cached_nodes.update({
      node_uid: {
        'original_relation_name': node_config.relation_name,
        'mocked_relation': mocked_relation
      }}
    ) -%}

  {%- endif -%}

  {# --If `mocked_sql` exists then this node has already been created and can be incremented. #}
  {%- set is_incremental_node = (
    node_config.get('config', {}).get('materialized') == 'incremental'
    and test_setup.get('options', {}).get('test_incremental', false)
    and mocked_sql is not none
  ) -%}

  {#/*
    If the node is incremental we still need to run its query again on top of the mocked data
    which will trigger the `is_incremental()` macro this time. This allows us to thoroughly
    test incremental nodes.
  */#}
  {%- if not mocked_sql or is_incremental_node -%}

    {# --If node is of type 'Seed' then it must be mocked as we can't build seeds nodes using dbt's test task context #}
    {%- if node_config.resource_type == 'seed' -%}
      {%- do dbt_flow.raise_error('Seed node "' ~ node_config.name ~ '" must have a mock as otherwise the node will not be built.') -%}
    {%- endif -%}

    {%- set raw_code = node_config.get('raw_code', '') -%}

    {# --We can't use the original `is_incremental()` macro so we mock it with dbt_flow's macros that simply returns `true` #}
    {%- if is_incremental_node -%}
        {%- set raw_code = raw_code | replace('is_incremental', 'dbt_flow.is_incremental') -%}
    {%- endif -%}

    {%- set sql = render(raw_code) -%}

    {%- set ns = namespace(sql = sql) -%}

    {%- for node_dependency in node_config.depends_on.nodes -%}
      
      {# --node has already been processed so we jump it #}
      {%- if node_dependency in cached_nodes -%}
        {%- continue -%}
      {%- else -%}
        {%- do dbt_flow.build_node(node_dependency, test_setup, cached_nodes) -%}
      {%- endif -%}

    {%- endfor -%} {# --loop of all node's dependencies #}

    {#/*
      We loop through all the cached nodes processed so far to update the sql
      query with the mocked new relations of the nodes. A more optimized
      approach would involve finding specifically which nodes belong to the
      graph dependency of the node being tested and then make the replacements
      but given that metric nodes do not have this informatino upfront then
      storing this information is not entirely straightforward. We opted then
      to simply loop through all nodes observed so far and replace whenever
      there's a match. It consumes more CPU power but it should be neglectable
      and it makes the code much easier to follow through.
    */#}
    {%- for node_dependency in cached_nodes -%}

      {%- set ns.sql = ns.sql | replace(
        cached_nodes[node_dependency]['original_relation_name'],
        cached_nodes[node_dependency]['mocked_relation'].render()) -%}

    {%- endfor -%}

    {# --If the query uses the {{ this }} macro we also need to replace it. #}
    {%- set ns.sql = ns.sql | replace(this, mocked_relation) -%}

    {# --some nodes are not supposed to be built as they don't have a raw_code sql(for instance seeds or sources) #}
    {%- if ns.sql -%}

      {%- if is_incremental_node -%}

        {%- do dbt_flow.create_incremental_table(relation=mocked_relation, sql=ns.sql, config=node_config) -%}
        {%- do dbt_flow.log(test_setup.test_name, 'Incremental mocked table "' ~ mocked_relation ~ '" for node "' ~ node_config.name ~ '" created successfully', test_setup.options.get('verbose', false)) -%}

      {%- else -%}

        {%- do dbt_flow.create_table(relation=mocked_relation, sql=ns.sql, config=node_config) -%}
        {%- do dbt_flow.log(test_setup.test_name, 'Mocked table "' ~ mocked_relation ~ '" for node "' ~ node_config.name ~ '" created successfully', test_setup.options.get('verbose', false)) -%}

      {%- endif -%}

      {%- do cached_nodes.update({
        node_uid: {
          'original_relation_name': node_config.relation_name,
          'mocked_relation': mocked_relation
        }
      }) -%}

    {%- endif -%}

  {%- endif -%}

{%- endmacro -%}


{#/*
  run_test(test_setup)

  Runs a specific test from the list of tests sent as input.

  Parameters
  ----------
  test_setup: Dict[str, str|Dict[str, Any]]
*/#}
{%- macro run_test(test_setup) -%}

  {%- do dbt_flow.log(test_setup.test_name, 'Starting test...', test_setup.options.get('verbose', false)) -%}
  {%- set target_node_config = dbt_flow.get_graph_node_config('name', test_setup.target_model) -%}

  {%- if not target_node_config -%}
    {%- do dbt_flow.raise_error('Input target model: "' ~ test_setup.target_model ~ '" in test: "' ~ test_setup.name ~ '" was not found in dbt graph.') -%}
  {%- endif -%}
   
  {%- set cached_nodes = {} -%}

  {%- set target_mocked_relation = api.Relation.create(database=target.database, schema=target.schema,
    identifier=dbt_flow.build_flow_identifier(test_name=test_setup.test_name, identifier=target_node_config.name)) -%}

  {%- set expected_mocked_relation = api.Relation.create(database=target.database, schema=target.schema,
    identifier=dbt_flow.build_flow_identifier(test_name=test_setup.test_name, identifier='expected')) -%}

  {%- do dbt_flow.create_table(relation=expected_mocked_relation, sql=test_setup.expected, config=target_node_config) -%}

  {%- do cached_nodes.update({
    'expected': {
      'original_relation_name': target_node_config.relation_name,
      'mocked_relation': expected_mocked_relation
    }}
  ) -%}

  {%- do dbt_flow.log(test_setup.test_name, 'Expected mocked table "' ~ target_mocked_relation ~ '" for node "' ~ target_node_config.name ~ '" created successfully', test_setup.options.get('verbose', false)) -%}

  {%- set columns_to_compare = adapter.get_columns_in_relation(expected_mocked_relation) -%}
  {%- set columns_to_compare = dbt_flow.process_expected_columns(columns_to_compare) -%}

  {#/*
    The contract of `build_node` accepts the unique id of the node instead of its config as this makes its code a bit
    simpler and more direct. This forces the target node to be scanned twice but as this task is quite cheap then
    it remained as is.
  */#}
  {%- do dbt_flow.build_node(target_node_config['unique_id'], test_setup, cached_nodes) -%}

  {%- set results = run_query(dbt_utils.test_equality(expected_mocked_relation, target_mocked_relation, columns_to_compare)) %}

  {%- do dbt_flow.process_test_results(results, test_setup.test_name) -%}

  {%- if test_setup.options.get('drop_tables', false) -%}
    {%- do dbt_flow.drop_mocked_tables(cached_nodes, test_setup.test_name, test_setup.options.get('verbose', false)) -%}
  {%- endif -%}

  {%- do dbt_flow.log(test_setup.test_name, 'Finished test!', test_setup.options.get('verbose', false)) -%}
  
{%- endmacro -%}

{#/*
  process_test_results(results, test_name)

  Parameters
  ----------
    results: agate.Table
             Results obtained from running the difference between expected table against
             observed real results. If there are differences it means that results are
             not as expected. In this case a logging message is printed and an error is
             raised.
    test_name: str
               Name of the current testing being run.
*/#}
{%- macro process_test_results(results, test_name) -%}

  {%- if results | length > 0 -%}

    {%- do log('\n' ~ '*' * 120 ~ '\n', True) -%}
    {%- do log('Test "' ~ test_name ~ '" failed. The following is the difference between expected(a) and observed(b):\n', True) -%}
    {%- do results.print_table(max_columns=None) -%}
    {%- do log('\n' ~ '*' * 120 ~ '\n', True) -%}
    {%- do dbt_flow.raise_error('Test "' ~ test_name ~ '" failed.') -%}

  {%- endif -%}

{%- endmacro -%}

{#/*
  run_tests(list_setups, global_options)

  Parameters
  ----------
  list_setups: Dict[str, str|Dict[str, Any]] | List[Dict[str, str|Dict[str, Any]]]
               Dict or list of dicts containing how each specific test should run. An example of input:

               dbt_flow.setup_test(
                 target_model='target_node_name_to_test',
                 name='name_of_test',
                 test_description='describes_the_test',
                 mocks={
                   "node1": """
                     select 1 as id, 'user1' as user union all
                     select 2 as id, 'user2' as user
                   """
                },
                 options={
                   drop_tables: False
                }
              )

              Please refer to dbt_flow.setup_test for more information.

  global_options: Dict[str, Any]
                These options work globally, that is, if a setup is defined here
                then all tests will follow this setting unless this same setting
                is also defined in the `options` key of the test or in the `var`
                "dbt_flow_config" section of the dbt_project.yml file.

                For example, it's possible to set in `global_options` whether the
                tables created during tests executing should be deleted or not:

                global_options = {"drop_tables": True}

                That would mean that all tests will drop the tables after finishing
                execution. But if in a given test the input is:

                dbt_flow.setup_test(
                  ...,
                  options={
                    "drop_tables": False
                  }
                )

                Then the inner most specific option will take precendence and for
                this specific test tables won't be deleted.

                If the same config is specified in the dbt_project.yml file, it
                takes the least precendence:

                vars:
                  dbt_flow_config:
                    drop_tables: false

                Therefore, dbt_project.yml < global_options < test_setup.options

  Raises
  ------
    If list_setups is not a list or is empty.
*/#}
{%- macro run_tests(list_setups, global_options={}) -%}

  {%- if execute -%}

    {%- if list_setups is mapping -%}
      {%- set list_setups = [list_setups] -%}
    {%- endif -%}

    {%- if list_setups is not iterable or list_setups|length == 0 -%}

      {%- do dbt_flow.raise_error("""
        No valid test setup was given to run the tests. Please make sure to first
        build the tests setups as a list of configurations and then run the tests
        by sending this same variable as input, i.e., `dbt_flow.run_tests(list_of_setups)`.
        """)
      -%}

    {%- endif -%}

    {#/*
      Create target schema if it doesn't already exist. We do so here as this would send
      just one query to the database instead of multiple ones case left for the `for` loop.
    */#}
    {%- if not adapter.check_schema_exists(database=target.database, schema=target.schema) -%}

      {%- do adapter.create_schema(api.Relation.create( database=target.database, schema=target.schema)) -%}

    {%- endif -%} 

    {%- for test_setup in list_setups -%}

      {%- do dbt_flow.update_test_options(test_setup, global_options) -%}
      {%- do dbt_flow.run_test(test_setup) -%}

    {%- endfor -%}

  {% endif %}

  {# --We need to return a 'no-op' query for dbt test to work #}
  {%- do return("select * from (select 1) as mock where false") -%}

{%- endmacro -%}
