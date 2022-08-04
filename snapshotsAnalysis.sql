/* 
    Prelimanary snapshot data validation
*/
USE gtx;
SELECT  
o.id AS observerid,
/*o.fullname,
s.contractId,*/
FROM_UNIXTIME(s.minute1_time -60) AS "Start Time",
FLOOR(MINUTE (FROM_UNIXTIME(s.minute1_time -60))/10)*10 AS "Start",
(s.last_minute_time - (s.minute1_time-60))/60 AS duration,
FROM_UNIXTIME(s.time) as "LAST MINUTE",
FROM_UNIXTIME(s.minute1_time) AS minute1,
TIME(FROM_UNIXTIME(s.minute2_time)) AS minute2,
TIME(FROM_UNIXTIME(s.minute3_time)) as minute3,
TIME(FROM_UNIXTIME(s.minute4_time)) as minute4,
TIME(FROM_UNIXTIME(s.minute5_time)) as minute5,
TIME(FROM_UNIXTIME(s.minute6_time)) as minute6,
TIME(FROM_UNIXTIME(s.minute7_time)) as minute7,
TIME(FROM_UNIXTIME(s.minute8_time)) as minute8,
TIME(FROM_UNIXTIME(s.minute9_time)) as minute9,
TIME(FROM_UNIXTIME(s.minute10_time)) as minute10,
s.mouseEventsCount,
s.minute1_mouse,
s.minute2_mouse,
s.last_minute_time-s.time,
FROM_UNIXTIME(s.minute2_time) AS minute2,
FROM_UNIXTIME(s.time) as "LAST MINUTE",
s.minute_instances,
s.minute2_time - s.minute1_time AS "minute2_time - minute1_time",
s.minute3_time - s.minute2_time AS "minute3_time - minute2_time",
s.minute4_time - s.minute3_time AS "minute4_time - minute3_time",
s.minute5_time - s.minute4_time AS "minute5_time - minute4_time",
s.minute6_time - s.minute5_time AS "minute6_time - minute5_time",
s.minute7_time - s.minute6_time AS "minute7_time - minute6_time",
s.minute8_time - s.minute7_time AS "minute8_time - minute7_time",
s.minute9_time - s.minute8_time AS "minute9_time - minute8_time",
s.minute10_time - s.minute9_time AS "minute10_time - minute9_time",
s.minute10_time - s.minute1_time AS "minute10_time - minute1_time", 
s.minute26_time

FROM  gtx.snapshots_qa s
JOIN observer o ON o.upwork_contractid = s.contractId
ORDER BY s.minute1_time