/*
    Observers' quality analysis
    Seperates pair and group observervatons
    Seperates observations on 20th

    Excel Analysis: https://docs.google.com/spreadsheets/d/15BlX-HNmoK3v0pK1Cr5l9yibllkoObroxIW0FSzT4F4/edit#gid=1300117420
*/

SELECT
obs.observerid, 
o.fullname,
/*SUM(CASE WHEN (v.count_complete_watched=2 AND crowd_agreed_count>2)   THEN 1 ELSE 0 END) AS "excess_matching",*/
SUM(CASE WHEN v.count_complete_watched=2                                  THEN 1 ELSE 0 END) AS "pair_observation_count",
SUM(CASE WHEN v.count_complete_watched=2 AND crowd_agreed_count>1         THEN 1 ELSE 0 END) AS "pair_crowd_agreed_count", 
                                        /* SHOULD MATCH EQUAL WHEN FIXED */
/*SUM(CASE WHEN v.count_complete_watched=2 AND crowd_agreed_count=2         THEN 1 ELSE 0 END) AS "pair_crowd_agreed_count_min",*/
/*SUM(CASE WHEN v.count_complete_watched=2 AND crowd_agreed_count>1         THEN 1 ELSE 0 END) -  SUM(CASE WHEN v.count_complete_watched=2 AND crowd_agreed_count=2         THEN 1 ELSE 0 END) - SUM(CASE WHEN (v.count_complete_watched=2 AND crowd_agreed_count>2)   THEN 1 ELSE 0 END) AS "0?",*/
SUM(CASE WHEN v.count_complete_watched=2                AND obs.is_orphan  THEN 1 ELSE 0 END) AS "pair_orphan_count",
COALESCE(SUM(CASE WHEN v.count_complete_watched=2       AND obs.is_orphan  THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched=2  THEN 1 ELSE 0 END)*100,0) AS "pair % of orphan observations",
COALESCE(SUM(CASE WHEN v.count_complete_watched=2 AND crowd_agreed_count>1 THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched=2  THEN 1 ELSE 0 END)*100,0) AS "pair % of agreed observations",

COALESCE((SELECT SUM (CASE WHEN NOT observerid=obs.observerid AND is_orphan AND stage ='production' THEN 1 WHEN NULL THEN 0 else 0 end)
        FROM gtx.observation 
        WHERE fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid 
        AND NOT DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched=2) 
        ),0) as pair_undetected_observations,
        
COALESCE((SELECT SUM (CASE WHEN NOT observerid=obs.observerid AND is_orphan AND stage ='production' THEN 1 WHEN NULL THEN 0 else 0 end)
        FROM gtx.observation 
        WHERE fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid 
        AND NOT DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched=2) 
        )/ SUM(CASE WHEN v.count_complete_watched=2                                  THEN 1 ELSE 0 END) * 100,0) as "pair_undetected_observations_per_observation X 100",     
        
        
SUM(CASE WHEN v.count_complete_watched>2                                  THEN 1 ELSE 0 END) AS "group_observation_count",
SUM(CASE WHEN v.count_complete_watched>2 AND crowd_agreed_count>1         THEN 1 ELSE 0 END) AS "group_crowd_agreed_count", 
SUM(CASE WHEN v.count_complete_watched>2                AND obs.is_orphan THEN 1 ELSE 0 END) AS "group_orphan_count",

COALESCE(SUM(CASE WHEN v.count_complete_watched>2       AND obs.is_orphan THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched>2  THEN 1 ELSE 0 END)*100,0) AS "group % of orphan observatoins",
COALESCE(SUM(CASE WHEN v.count_complete_watched>2 AND crowd_agreed_count>1 THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched>2  THEN 1 ELSE 0 END)*100,0) AS "group % of agreed observations",


COALESCE((SELECT COUNT(distinct better_obsid)
        FROM gtx.observation 
        WHERE NOT better_obsid IN (SELECT better_obsid FROM gtx.observation WHERE observerid=obs.observerid) 
        AND NOT observerid=obs.observerid AND crowd_agreed_count>2 AND stage ='production'
        AND fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid AND NOT DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched>2) 
        AND NOT(fileid  IN (SELECT fileid FROM gtx.video_action WHERE watched_observation_count=0)
        AND obs.observerid IN (SELECT observerid FROM gtx.video_action WHERE watched_observation_count=0))
        ),0) as undetected_crowd_agreed_observations,
        
COALESCE((SELECT COUNT(distinct better_obsid)
        FROM gtx.observation 
        WHERE NOT better_obsid IN (SELECT better_obsid FROM gtx.observation WHERE observerid=obs.observerid) 
        AND NOT observerid=obs.observerid AND crowd_agreed_count>2 AND stage ='production'
        AND fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid AND NOT DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched>2) 
        AND NOT(fileid  IN (SELECT fileid FROM gtx.video_action WHERE watched_observation_count=0)
        AND obs.observerid IN (SELECT observerid FROM gtx.video_action WHERE watched_observation_count=0))
        )/SUM(CASE WHEN v.count_complete_watched>2                                  THEN 1 ELSE 0 END)*100,0) AS 'undetected_crowd_agreed_observations_per_observation X 100'

        
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
AND NOT DAY(dt_seen_local) = 20
AND    v.daynight = 'D'
GROUP BY obs.observerid;

SELECT 
obs.observerid, 
o.fullname, 
/*SUM(CASE WHEN v.count_complete_watched=2                 THEN 1 ELSE 0 END) AS "pair_observation_count",
SUM(CASE WHEN v.count_complete_watched=2  AND is_orphan  THEN 1 ELSE 0 END) AS "pair_orphan_count",
COALESCE(SUM(CASE WHEN v.count_complete_watched=2 AND is_orphan THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched=2  THEN 1 ELSE 0 END)*100,0) AS "% of orphans",*/
SUM(CASE WHEN v.count_complete_watched>2                                  THEN 1 ELSE 0 END) AS "group_observation_count",
SUM(CASE WHEN v.count_complete_watched>2 AND crowd_agreed_count>1         THEN 1 ELSE 0 END) AS "group_crowd_agreed_count", 
SUM(CASE WHEN v.count_complete_watched>2                AND obs.is_orphan THEN 1 ELSE 0 END) AS "group_orphan_count",
COALESCE(SUM(CASE WHEN v.count_complete_watched>2       AND obs.is_orphan THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched>2  THEN 1 ELSE 0 END)*100,0) AS "group % of orphan observatoins",
COALESCE(SUM(CASE WHEN v.count_complete_watched>2 AND crowd_agreed_count>1 THEN 1 ELSE 0 END)/SUM(CASE WHEN v.count_complete_watched>2  THEN 1 ELSE 0 END)*100,0) AS "group % of agreed observations",



COALESCE((SELECT COUNT(distinct better_obsid)
        FROM gtx.observation 
        WHERE NOT better_obsid IN (SELECT better_obsid FROM gtx.observation WHERE observerid=obs.observerid) 
        AND NOT observerid=obs.observerid AND crowd_agreed_count>2 AND stage ='production'
        AND fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid AND DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched>2) 
        AND NOT(fileid  IN (SELECT fileid FROM gtx.video_action WHERE watched_observation_count=0)
        AND obs.observerid IN (SELECT observerid FROM gtx.video_action WHERE watched_observation_count=0))
        ),0) as undetected_crowd_agreed_observations,
        
COALESCE((SELECT COUNT(distinct better_obsid)
        FROM gtx.observation 
        WHERE NOT better_obsid IN (SELECT better_obsid FROM gtx.observation WHERE observerid=obs.observerid) 
        AND NOT observerid=obs.observerid AND crowd_agreed_count>2 AND stage ='production'
        AND fileid IN (SELECT fileid FROM gtx.observation WHERE observerid=obs.observerid AND DAY(dt_seen_local) = 20) 
        AND fileid IN (SELECT id FROM gtx.video WHERE count_complete_watched>2) 
        AND NOT(fileid  IN (SELECT fileid FROM gtx.video_action WHERE watched_observation_count=0)
        AND obs.observerid IN (SELECT observerid FROM gtx.video_action WHERE watched_observation_count=0))
        )/SUM(CASE WHEN v.count_complete_watched>2                                  THEN 1 ELSE 0 END)*100,0) AS 'undetected_crowd_agreed_observations_per_observation X 100'
/**/
FROM   gtx.observation obs  
JOIN   observer o ON o.id = obs.observerid 
JOIN   video v    ON v.id = obs.fileid
Right JOIN   video_action va   ON va.observerid = obs.observerid AND va.fileid = obs.fileid AND va.fileid =v.id AND va.observerid=o.id
WHERE  obs.stage='production'     
AND    o.stage='production'
AND    v.stage='production'
AND    va.stage='production'
AND    v.id = obs.fileid 
AND    DAY(dt_seen_local) = 20
AND    v.daynight = 'D'
GROUP BY obs.observerid, va.observerid;
