/*
    Details all orphan observations for review 
    Excludes 2 observers that label cars going right to left
 */
SELECT
obs.id,
obs.observerid, 
o.fullname,
obs.fileid,
obs.vehicle_type,
obs.dt_seen_local,
obs.time_in_video

        
FROM   gtx.observation obs  
RIGHT OUTER JOIN   observer o ON o.id = obs.observerid 
JOIN   video v    ON v.id = obs.fileid
JOIN   video_action va   ON va.fileid= obs.fileid AND va.observerid = obs.observerid
WHERE  obs.stage='production'     
AND    o.stage='production'
AND    v.stage='production'
AND    va.stage='production'
AND    v.id = obs.fileid 
AND    o.id = va.observerid
AND    obs.observerid NOT IN (45, 46)
AND    is_orphan
AND    v.count_complete_watched>1
AND    v.daynight = 'D'
ORDER BY 6,7