{###
Load SCD type 2 into worker dimension dim_worker_scd2 table
example for one day: 
dbt snapshot -m dim_worker_type2 --vars '{ "ctrm_odate": "2022-01-01"}'
I am not using invalidate_hard_deletes=True because our data is incremental only
###}

{% snapshot dim_worker %}
{% set src_filter_condition = "TO_DATE('" + var('ctrm_odate') + "','" + "YYYY-MM-DD" + "')" %}
{{
    config(
        invalidate_hard_deletes=False,
        target_schema='DWH',
        unique_key="WORKERID||'-'||ADDITIONALJOBS_ID",
        check_cols=['ADDITIONALJOBS_LOCATION'],
        strategy='check',
        tags='worker_dimension',
        alias='dim_worker_scd2',
        
    )
}}
with dedup_source_query as
(
select 
	WD_AS_OF_DATE,
    row_number() over (partition by WORKERID, ADDITIONALJOBS_ID order by WD_AS_OF_DATE desc) rn,
	PRIMARYJOB_BUSINESSTITLE ,
	PRIMARYJOB_JOBPROFILEID ,
	PRIMARYJOB_LOCATION ,
	PERSON_ID ,
	PERSON_PHONE ,
	PERSON_EMAIL ,
	WORKERID ,
	HIREDATE ,
	TERMINATEDATE ,
	ADDITIONALJOBS_BUSINESSTITLE ,
	ADDITIONALJOBS_JOBPROFILEID ,
	ADDITIONALJOBS_LOCATION ,
	ADDITIONALJOBS_ID 
    from {{ ref('get_worker_stg') }}
    where 
        1=1
        {% if src_filter_condition  is not none %} 
            AND WD_AS_OF_DATE = {{ src_filter_condition }} 
        {% endif %}
)

select 
	WD_AS_OF_DATE,
 	PRIMARYJOB_BUSINESSTITLE ,
	PRIMARYJOB_JOBPROFILEID ,
	PRIMARYJOB_LOCATION ,
	PERSON_ID ,
	PERSON_PHONE ,
	PERSON_EMAIL ,
	WORKERID ,
	HIREDATE ,
	TERMINATEDATE ,
	ADDITIONALJOBS_BUSINESSTITLE ,
	ADDITIONALJOBS_JOBPROFILEID ,
	ADDITIONALJOBS_LOCATION ,
	ADDITIONALJOBS_ID 
from dedup_source_query
where rn=1 

{% endsnapshot %}