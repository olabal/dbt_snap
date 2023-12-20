{###
This test is to check duplicate data in stage for a particular snapshot date
###}

{% set src_filter_condition = "TO_DATE('" + var('ctrm_odate') + "','" + "YYYY-MM-DD" + "')" %}
select 
	WD_AS_OF_DATE,
    WORKERID ,
	ADDITIONALJOBS_JOBPROFILEID ,
	count(*) as count_dups
    from {{ source('stage_data','get_worker_stg') }}
    where 
        1=1
        {% if src_filter_condition  is not none %} 
            AND WD_AS_OF_DATE = {{ src_filter_condition }} 
        {% endif %}
    group by WD_AS_OF_DATE, WORKERID, ADDITIONALJOBS_JOBPROFILEID 
    having count(*) > 1