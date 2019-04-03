imposing-union-227917:sumo.twitter_frt

select tr.* from
(select *, TIMESTAMP_DIFF(created_at,in_reply_to_status_created_at,SECOND) response_time from `imposing-union-227917`.sumo.twitter_reviews) tr
inner join
(select in_reply_to_status_id_str, min(TIMESTAMP_DIFF(created_at,in_reply_to_status_created_at,SECOND)) ts_diff from `imposing-union-227917`.sumo.twitter_reviews 
group by in_reply_to_status_id_str) frt
on tr.in_reply_to_status_id_str = frt.in_reply_to_status_id_str and tr.response_time=frt.ts_diff
