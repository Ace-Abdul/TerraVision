/* 
    Validating that snapshot rows x10 = minutes billed by upwork
*/
USE gtx;
SELECT  
o.id AS observerid,
ub.upwork_name,
DATE(FROM_UNIXTIME(s.time)) as "DATE",
COUNT(o.id) AS "# snapshots",
ROUND(COUNT(o.id)*10/60,2) AS "snapshot hours",
ub.hours AS "upwork hours",
ub.hours - ROUND(COUNT(o.id)*10/60,2) AS "upwork hours - snapshot hours"


FROM  gtx.snapshots_qa s
JOIN observer o ON o.upwork_contractid = s.contractId
JOIN upwork_billed ub ON  ub.date_worked = DATE(FROM_UNIXTIME(s.time)) AND ub.upwork_name = o.upwork_name
GROUP BY DATE(FROM_UNIXTIME(s.time)),1
ORDER BY 3,1;
