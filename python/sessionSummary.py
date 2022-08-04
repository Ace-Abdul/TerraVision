import sys
import argparse
import json
import pymysql

class sessionSummary:
    def __init__(self, row):
        self.contract = row["contractId"]
        self.date = row["date_worked"]
        self.name = row["name"]
        self.cntSessions = 1
        self.cntnSegments = 1
        self.cnt = row['minute_instances']
        self.workedMins = row['minDuration']
        self.billedMins = 10
        self.paddedMins = 10-self.workedMins
        self.paddedMinsRatio = self.paddedMins/self.billedMins
        self.inactiveMins = 0
        self.cntMouse = 0
        self.cntKeybrd = 0
        self.unreportedMins = 0
        self.diff = None
        for i in range(1, row['minute_instances']+1):
            self.cntMouse += row[f"minute{i}_mouse"]
            self.cntKeybrd += row[f"minute{i}_keyboard"]
            if row[f"minute{i}_mouse"] ==0 and row[f"minute{i}_keyboard"]==0:
                self.inactiveMins+=1
            if i<row['minute_instances'] and row[f"{i}"]>70:
                self.unreportedMins +=1

        self.activeMins = self.cnt - self.inactiveMins
        self.avgMouse = self.cntMouse/self.workedMins
        self.avgKeybrd = self.cntKeybrd/self.workedMins
        self.startSess = row["minute0"]
        self.endSess = row["end"]
    
    def newSegment(self, row):
        self.workedMins += row['minDuration']
        self.cntnSegments += 1
        self.billedMins += 10
        self.paddedMins = self.billedMins - self.workedMins
        self.paddedMinsRatio = self.paddedMins/self.billedMins
        self.cnt += row['minute_instances']
        for i in range(1, row['minute_instances']+1):
            self.cntMouse += row[f"minute{i}_mouse"]
            self.cntKeybrd += row[f"minute{i}_keyboard"]
            if row[f"minute{i}_mouse"] ==0 and row[f"minute{i}_keyboard"]==0:
                self.inactiveMins+=1
            if i <row['minute_instances'] and row[f"{i}"]>70:
                self.unreportedMins+=1
        
        self.activeMins = self.cnt - self.inactiveMins
        self.avgMouse = self.cntMouse/self.workedMins
        self.avgKeybrd = self.cntKeybrd/self.workedMins
        self.endSess = row["end"]
        if row["state"] == "new":
            self.cntSessions += 1


def openDB(fyle="ace.json"):
    cfg=None
    try:
            with open(fyle, "r") as json_file:
                cfg = json.load(json_file)
            sql="""
                        SELECT  
                        o.id AS observerid,
                        o.upwork_name as "name",
                        s.contractId,
                        s.date_worked,
                        FROM_UNIXTIME(s.minute1_time - 60) AS minute0,
                        SEC_TO_TIME(s.minute1_time-60 - (LAG(s.time,1) OVER(PARTITION BY o.id, DATE(FROM_UNIXTIME(s.time)) ORDER BY s.time)) ) AS "diff",
                        LAG(TIME(FROM_UNIXTIME(s.time)),1) OVER(PARTITION BY o.id, DATE(FROM_UNIXTIME(s.time)) ORDER BY s.time) "previous end time",
                        ROUND((s.time - (s.minute1_time -60))/60,2) AS minDuration,
                        SEC_TO_TIME(s.time - (LAG(s.time,1) OVER(PARTITION BY o.id, DATE(FROM_UNIXTIME(s.time)) ORDER BY s.time)) ) AS duration2,
                        FROM_UNIXTIME(s.time) AS "end",
                        (CASE 
                                WHEN (((s.minute1_time-60 - (LAG(s.time,1) OVER(PARTITION BY o.id, DATE(FROM_UNIXTIME(s.time)) ORDER BY s.time))) <60) 
                                AND ((s.minute1_time-60 - (LAG(s.time,1) OVER(PARTITION BY o.id, DATE(FROM_UNIXTIME(s.time)) ORDER BY s.time)) > -60)) )
                                THEN "continious" 
                                ELSE "new" END) AS "state",
                        s.minute_instances,
                        minute1_mouse, minute2_mouse, minute3_mouse, minute4_mouse, minute5_mouse, minute6_mouse, minute7_mouse, minute8_mouse, minute9_mouse, minute10_mouse, minute11_mouse, minute12_mouse, minute13_mouse, minute14_mouse, minute15_mouse, minute16_mouse, minute17_mouse, minute18_mouse, minute19_mouse, minute20_mouse, minute21_mouse, minute22_mouse, minute23_mouse, minute24_mouse, minute25_mouse, minute26_mouse, minute27_mouse,
                        minute1_keyboard, minute2_keyboard, minute3_keyboard, minute4_keyboard, minute5_keyboard, minute6_keyboard, minute7_keyboard, minute8_keyboard, minute9_keyboard, minute10_keyboard, minute11_keyboard, minute12_keyboard, minute13_keyboard, minute14_keyboard, minute15_keyboard, minute16_keyboard, minute17_keyboard, minute18_keyboard, minute19_keyboard, minute20_keyboard, minute21_keyboard, minute22_keyboard, minute23_keyboard, minute24_keyboard, minute25_keyboard, minute26_keyboard, minute27_keyboard,
                        s.minute2_time - s.minute1_time AS '1', s.minute3_time - s.minute2_time AS '2', s.minute4_time - s.minute3_time AS '3', s.minute5_time - s.minute4_time AS '4', s.minute6_time - s.minute5_time AS '5', s.minute7_time - s.minute6_time AS '6', s.minute8_time - s.minute7_time AS '7', s.minute9_time - s.minute8_time AS '8', s.minute10_time - s.minute9_time AS '9', s.minute11_time - s.minute10_time AS '10', s.minute12_time - s.minute11_time AS '11', s.minute13_time - s.minute12_time AS '12', s.minute14_time - s.minute13_time AS '13', s.minute15_time - s.minute14_time AS '14', s.minute16_time - s.minute15_time AS '15', s.minute17_time - s.minute16_time AS '16', s.minute18_time - s.minute17_time AS '17', s.minute19_time - s.minute18_time AS '18', s.minute20_time - s.minute19_time AS '19', s.minute21_time - s.minute20_time AS '20', s.minute22_time - s.minute21_time AS '21', s.minute23_time - s.minute22_time AS '22', s.minute24_time - s.minute23_time AS '23', s.minute25_time - s.minute24_time AS '24', s.minute26_time - s.minute25_time AS '25', s.minute27_time - s.minute26_time AS '26'

                        FROM  gtx.snapshots_qa s
                        JOIN observer o ON o.upwork_contractid = s.contractId
                        ORDER BY o.id, s.minute1_time"""

            db_conn = pymysql.connect(host=cfg["dbserver"], user=cfg["dbuser"], passwd= cfg["dbpassword"], db="gtx", connect_timeout=35)
            db_cur = db_conn.cursor(pymysql.cursors.DictCursor)
            print("Database Accessed")
            db_cur.execute(sql)
            return db_conn, db_cur, db_cur.fetchall()

    except Exception as e:
            print("Database Connection Failed ==> ERROR: %s" % str(e))
            sys.exit(1)

def writeDB(db_conn, db_cur, entry):
    db_cur.execute(f"SELECT * FROM gtx.upwork_billed WHERE date_worked = '{entry.date}' AND upwork_name = '{entry.name}'")
    ub = db_cur.fetchone()
    if ub:
        entry.diff = round(ub["hours"]*60) - entry.billedMins
        print(entry.diff)
        sql = """ INSERT INTO gtx.upwork_daily_summary_qa (contractid, upwork_name, date_worked, contract_title, team, hours, charge, upwork_billedid, dt_start, dt_end, cnt_sessions, cnt_segments, mins_upwork_billed, workdiary_mins_not_billed, mins_snapshot_duration, mins_upwork_padded, cnt_mins_unreported_in_snapshots, cnt_mins_active, cnt_mins_inactive, avg_mouse, avg_keys, mins_padded_per_billed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values = (entry.contract, entry.name, entry.date, ub["contract_title"], ub["team"], ub["hours"], ub["charge"], ub["id"], entry.startSess, entry.endSess, entry.cntSessions, entry.cntnSegments, entry.billedMins, entry.diff, entry.workedMins, entry.paddedMins, entry.unreportedMins, entry.activeMins, entry.inactiveMins, entry.avgMouse, entry.avgKeybrd,entry.paddedMinsRatio)
        db_cur.execute(sql,values)
    else:
        sql = """ INSERT INTO gtx.upwork_daily_summary_qa (contractid, upwork_name, date_worked, dt_start, dt_end, cnt_sessions, cnt_segments, mins_upwork_billed, workdiary_mins_not_billed, mins_snapshot_duration, mins_upwork_padded, cnt_mins_unreported_in_snapshots, cnt_mins_active, cnt_mins_inactive, avg_mouse, avg_keys,mins_padded_per_billed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values = (entry.contract, entry.name, entry.date, entry.startSess, entry.endSess, entry.cntSessions, entry.cntnSegments, entry.billedMins, entry.diff, entry.workedMins, entry.paddedMins, entry.unreportedMins, entry.activeMins, entry.inactiveMins, entry.avgMouse, entry.avgKeybrd,entry.paddedMinsRatio)
        db_cur.execute(sql,values)

def closeDB(db_conn, db_cur):
        db_conn.commit()
        db_conn.close()
        print("Database Closed\n*** Mission Passed: Respect + ***")

def _parse_args():
        parser = argparse.ArgumentParser(description='Ingestor',
                                        formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument(
            '--config',
            dest='config',
            # required=True,
            help='Relative or fully qualified path to the .JSON configuration file to use',
            default = "ace.json"
        )

        return parser.parse_args()

if __name__ == '__main__':
    args = _parse_args()
    fyle= args.config
    [db_conn,db_cur,query]=openDB(fyle)
    sessions = {}

    prev = None
    crnt = None
    x=0
    for row in query:
        
        if row["contractId"] not in sessions.keys() or not sessions[row["contractId"]] == row["date_worked"]:
            prev = crnt
            crnt=sessionSummary(row)
            sessions[crnt.contract] = row["date_worked"]
            x+=1
            if x>1:
                writeDB(db_conn, db_cur, prev)
                print(prev.contract,prev.name, prev.date,prev.cntnSegments,prev.unreportedMins, prev.cntSessions)

        else:
            crnt.newSegment(row)
            
    
    closeDB(db_conn, db_cur)