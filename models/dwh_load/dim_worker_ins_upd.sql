{###
Load SCD type 1 into worker dimension dim_worker_scd1 table, using alias
example for one day: 
-----------dbt run --select tag:worker_dimension --vars '{ "ctrm_odate": "2022-01-02"}'
dbt run --select dim_worker_ins_upd --vars '{ "ctrm_odate": "2022-01-01"}'
This model uses incremental materialization.
We need this model to run incremental functionality (merge statement).
###}

select * from {{ ref('get_worker_int') }}

