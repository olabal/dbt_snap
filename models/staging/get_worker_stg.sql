{###
Purpose : parse raw json data for a as_of_date snapshot 
Version : 1
###}

{% set src_filter_condition = "TO_DATE('" + var('ctrm_odate') + "','" + "YYYY-MM-DD" + "')" %}

SELECT
TO_DATE(gw.src_data:wd_as_of_date::VARCHAR(10),'YYYY-MM-DD') as wd_as_of_date,
persons.value:primaryJob.businessTitle::VARCHAR(20)       as primaryJob_businessTitle,
persons.value:primaryJob.jobProfileID::VARCHAR(20)        as primaryJob_jobProfileID,  
persons.value:primaryJob.location::VARCHAR(20)            as primaryJob_location,
persons.value:person.id::VARCHAR(20)                      as person_id,
persons.value:person.phone::VARCHAR(20)                   as person_phone,
persons.value:person.email::VARCHAR(20)                   as person_email,
persons.value:workerId::NUMBER(10)                       as workerId,
TO_DATE(persons.value:hireDate::VARCHAR(10),'YYYY-MM-DD')                       as hireDate,
COALESCE(TO_DATE(persons.value:terminateDate::VARCHAR(10),'YYYY-MM-DD'), TO_DATE('9999-12-31','YYYY-MM-DD') )  as terminateDate,
add_jobs.value:businessTitle::VARCHAR(20)                 as additionalJobs_businessTitle,
add_jobs.value:jobProfileID::VARCHAR(20)                  as additionalJobs_jobProfileId,
add_jobs.value:location::VARCHAR(20)                      as additionalJobs_location,
add_jobs.value:id::VARCHAR(20)                            as additionalJobs_id
FROM {{ source('raw_data','get_worker') }} gw
,LATERAL FLATTEN(INPUT => gw.src_data:data, MODE => 'ARRAY') persons 
,LATERAL FLATTEN(INPUT => persons.value:additionalJobs, MODE => 'ARRAY') add_jobs
WHERE 1=1
 
{% if src_filter_condition  is not none %} 
 AND TO_DATE(gw.src_data:wd_as_of_date::VARCHAR(10),'YYYY-MM-DD') = {{ src_filter_condition }} 
{% endif %} 

{## example of a file from get_workers Workday rest API
https://community.workday.com/sites/default/files/file-hosting/restapi/index.html#timeTracking/v3/get-/workers

{   "wd_as_of_date": "2022-01-01",
    "data": [
      {
        "primaryJob": {
          "businessTitle": "Data engineer",
          "jobProfileID": "JP1",
          "location": "USA"
        },
        "person": {
          "phone": "+1 (800)717-5000 ",
          "email": "dataeng1@gmail.com",
          "id": "PERSON1"
        },
        "workerId": "100",
        "hireDate": "2020-01-01",
        "terminateDate": null,
        "additionalJobs": [
          {
            "businessTitle": "Data engineer p1",
            "jobProfileID": "JP1",
            "location": "USA",
            "id": "ADD_JOB_1"
          },
          {
            "businessTitle": "Data analyst p1",
            "jobProfileID": "JP2",
            "location": "USA",
            "id": "ADD_JOB_2"
          }
	]
	}
##}