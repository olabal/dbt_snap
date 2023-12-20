{###
Purpose : prepare worker dimension intermediate data for type 1 incremental model, fill out our system fields: INSERT_TS, UPDATE_TS, ETL_GMTS .
We are capturing changes in source fields using MD5 function.
New rows are going to be inserted, changed existing rows are going to be updated.
Version : 1
###}

{% set src_filter_condition = "TO_DATE('" + var('ctrm_odate') + "','" + "YYYY-MM-DD" + "')" %}

with STAGE_INC as
(
    SELECT 
    STG_INC.*, 
    md5( 
        coalesce(cast(WORKERID as varchar), '') || '-' || coalesce(cast(PRIMARYJOB_BUSINESSTITLE as varchar), '') || 
        '-' || coalesce(cast(PRIMARYJOB_JOBPROFILEID as varchar), '') || '-' || coalesce(cast(PRIMARYJOB_LOCATION as varchar), '') || 
        '-' || coalesce(cast(PERSON_ID as varchar), '') ||'-' || coalesce(cast(PERSON_PHONE as varchar), '') || 
        '-' || coalesce(cast(ADDITIONALJOBS_JOBPROFILEID as varchar), '') || '-' || coalesce(cast(ADDITIONALJOBS_LOCATION as varchar), '') 
        ) as STG_MD5_SK, 
    WD_AS_OF_DATE as INSERT_TS,
    WD_AS_OF_DATE as UPDATE_TS,
    current_timestamp() as ETL_GMTS
    FROM {{ ref('get_worker_stg') }} STG_INC
    WHERE
            1=1
        {% if src_filter_condition  is not none %} 
            AND WD_AS_OF_DATE = {{ src_filter_condition }} 
        {% endif %}
    
),

SRC_JOINED_TGT as
( 
SELECT 
STG.WD_AS_OF_DATE,
STG.STG_MD5_SK ,
STG.PRIMARYJOB_BUSINESSTITLE ,
STG.PRIMARYJOB_JOBPROFILEID ,
STG.PRIMARYJOB_LOCATION ,
STG.PERSON_ID ,
STG.PERSON_PHONE ,
STG.PERSON_EMAIL ,
STG.WORKERID ,
STG.HIREDATE ,
STG.TERMINATEDATE ,
STG.ADDITIONALJOBS_BUSINESSTITLE ,
STG.ADDITIONALJOBS_JOBPROFILEID ,
STG.ADDITIONALJOBS_LOCATION ,
STG.ADDITIONALJOBS_ID,
NULL as UPDATE_TS,
NULL as INSERT_TS,
ETL_GMTS
FROM STAGE_INC STG
LEFT OUTER JOIN {{ source('dwh_data', 'dim_worker_scd1') }} TARGET_ 
--JOIN on Natural keys
ON STG.WORKERID = TARGET_.WORKERID AND STG.ADDITIONALJOBS_ID = TARGET_.ADDITIONALJOBS_ID 
WHERE 
STG.STG_MD5_SK <> TARGET_.STG_MD5_SK  OR TARGET_.WORKERID IS NULL
)
SELECT * FROM SRC_JOINED_TGT

