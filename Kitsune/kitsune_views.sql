imposing-union-227917.sumo.kitsune_questions

SELECT latest_row.*
FROM (
  SELECT ARRAY_AGG(t ORDER BY updated DESC LIMIT 1)[OFFSET(0)] latest_row
  FROM `imposing-union-227917.sumo.kitsune_questions_raw` as t
  GROUP BY t.question_id
) 


imposing-union-227917.sumo.kitsune_answers

--select answer_id, count(*) from sumo.kitsune_answers group by answer_id having count(*) > 1
SELECT latest_row.*
FROM (
  SELECT ARRAY_AGG(t ORDER BY updated DESC LIMIT 1)[OFFSET(0)] latest_row
  FROM `imposing-union-227917.sumo.kitsune_answers_raw` as t
  GROUP BY t.answer_id
  

imposing-union-227917.sumo.kitsune_frt

SELECT latest_row.*
FROM (
  SELECT ARRAY_AGG(aq_rt ORDER BY frt ASC LIMIT 1)[OFFSET(0)] latest_row 
  FROM (
    select a.*, q_created_date, q_updated, TIMESTAMP_DIFF(a_created_date,q_created_date,SECOND) frt from 
    (select answer_id, question_id, created_date as a_created_date, updated as a_updated from `imposing-union-227917.sumo.kitsune_answers`) a
    left join
    (select question_id, created_date as q_created_date, updated as q_updated from `imposing-union-227917.sumo.kitsune_questions`) q
    on a.question_id=q.question_id
    where q_created_date is not null
  ) aq_rt
  group by question_id
)


imposing-union-227917.sumo.kitsune_greater_than_24h_response

SELECT * from `imposing-union-227917.sumo.kitsune_questions`
WHERE question_id NOT IN 
(SELECT question_id FROM `imposing-union-227917.sumo.kitsune_frt` 
where frt <= 86400)
