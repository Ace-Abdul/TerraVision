/*
    Prelimanary truth in billing analysis

    Excel analysis: https://docs.google.com/spreadsheets/d/1yt8yqBVmqmtdALeeBG9lO-yp5AxQJDVqXVQevrhcaO4/edit#gid=57541012
*/
SELECT 
DATE(va.dt_first_play_utc) AS "date",
va.observerid,
o.upwork_name AS "upwork name",
ROUND((SUM( distinct CASE WHEN  va.dt_first_play_utc THEN  (TIME_TO_SEC(va.dt_action_ended_utc) - TIME_TO_SEC(va.dt_first_play_utc))
         ELSE 0 END))/3600,2) AS "hours worked db",
(ub.hours) AS "hours worked upwork",
(CASE  WHEN SUM( distinct CASE WHEN  va.dt_first_play_utc THEN  (TIME_TO_SEC(va.dt_action_ended_utc) - TIME_TO_SEC(va.dt_first_play_utc))
   ELSE 0 END)/3600 < (ub.hours) THEN (ub.hours) - ROUND(SUM( distinct CASE WHEN  va.dt_first_play_utc THEN  (TIME_TO_SEC(va.dt_action_ended_utc) - TIME_TO_SEC(va.dt_first_play_utc)) ELSE 0 END)/3600,2)
                ELSE NULL END
                 )AS "upwork hours - db hours",
COUNT(distinct va.fileid) AS "number of files watched",
GROUP_CONCAT(distinct va.fileid) AS "list of files watched"


FROM    gtx.video_action va
JOIN    gtx.observation obs ON obs.observerid = va.observerid AND obs.fileid = va.fileid
JOIN    gtx.upwork_billed ub ON ub.date_worked = DATE(va.dt_first_play_utc)
JOIN    gtx.observer o ON o.upwork_name = ub.upwork_name AND va.observerid = o.id
AND     va.dt_abandoned_utc IS NULL
AND     va.stage = 'production'
AND     va.watched_observation_count>0
AND     va.dt_first_play_utc
GROUP BY 1,2
HAVING (ROUND((SUM( distinct CASE WHEN  va.dt_first_play_utc THEN  (TIME_TO_SEC(va.dt_action_ended_utc) - TIME_TO_SEC(va.dt_first_play_utc)) ELSE 0 END))/3600,2))>0 AND SUM( distinct CASE WHEN  va.dt_first_play_utc THEN  (TIME_TO_SEC(va.dt_action_ended_utc) - TIME_TO_SEC(va.dt_first_play_utc))
   ELSE 0 END)/3600 < (ub.hours)
ORDER BY 1,2,4; 