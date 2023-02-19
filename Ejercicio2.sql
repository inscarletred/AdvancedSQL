WITH ivr_summary 

AS (SELECT ivr_detail.ivr_id, 
      ivr_detail.phone_number, 
      ivr_detail.ivr_result,


  IF(LEFT(vdn_label, 3) = 'ATC', 'FRONT', 
     IF(LEFT(vdn_label, 4) = 'TECH', 'TECH', 
        IF(vdn_label LIKE 'ABSORTION%', 'ABSORTION', 'RESTO'))) AS vdn_aggregation,
        ivr_detail.start_date,
        ivr_detail.end_date,
        ivr_detail.total_duration,
        ivr_detail.customer_segment,
        ivr_detail.ivr_language,
        ivr_detail.steps_module,
        ivr_detail.module_aggregation,
      MAX(NULLIF(document_type, 'NULL')) AS document_type,
      MAX(NULLIF(document_identification, 'NULL')) AS document_identification,
      MAX(NULLIF(customer_phone, 'NULL')) AS customer_phone,
      MAX(NULLIF(billing_account_id, 'NULL')) AS billing_account_id,
      MAX(IF(module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg,
      CASE WHEN MAX(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error = 'NULL')
            THEN 1
            ELSE 0
            END AS info_by_phone_lg,
      CASE WHEN MAX(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_description_error = 'NULL')
            THEN 1
            ELSE 0
            END AS info_by_dni_lg
      

FROM keepcoding.ivr_detail
GROUP BY ivr_detail.ivr_id, ivr_detail.phone_number, ivr_detail.ivr_result,
         vdn_aggregation, ivr_detail.start_date, ivr_detail.end_date,
         ivr_detail.total_duration, ivr_detail.customer_segment,
         ivr_detail.ivr_language, ivr_detail.steps_module, ivr_detail.module_aggregation)
, call_back
      AS (SELECT a.ivr_id, a.phone_number, 
            CASE 
            WHEN COUNT(DISTINCT b.ivr_id) >= 1 THEN 1
            ELSE 0
            END AS same_phone_24H
FROM keepcoding.ivr_summary a
LEFT JOIN keepcoding.ivr_summary b ON a.phone_number = b.phone_number 
                  AND a.ivr_id <> b.ivr_id 
                  AND TIMESTAMP_DIFF(a.start_date, b.start_date, HOUR) BETWEEN 0 AND 24
                  
GROUP BY a.ivr_id, a.phone_number

  )

  ,case_call_back

AS (SELECT a.ivr_id, a.phone_number,
            CASE 
            WHEN COUNT(DISTINCT b.ivr_id) >= 1 THEN 1
            ELSE 0
            END AS case_recall_phone_24H
FROM keepcoding.ivr_summary a
LEFT JOIN keepcoding.ivr_summary b ON a.phone_number = b.phone_number 
                  AND a.ivr_id <> b.ivr_id 
                  AND TIMESTAMP_DIFF( b.start_date,a.start_date, HOUR) BETWEEN 0 AND 24
                  
GROUP BY a.ivr_id, a.phone_number)

  SELECT ivr_summary.*, call_back.same_phone_24H, case_call_back.case_recall_phone_24H FROM ivr_summary 
  LEFT JOIN call_back 
  ON ivr_summary.ivr_id = call_back.ivr_id 
  LEFT JOIN case_call_back
  ON ivr_summary.ivr_id = case_call_back.ivr_id
ORDER BY phone_number, start_date


