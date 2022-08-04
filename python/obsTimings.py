import sys
import argparse
import json
import pymysql


class obsTime():
    def __init__(self,row):
        if row["duration"]==None:
            row["duration"] = 0
        self.oid = row["observerid"]
        self.date = row['date']
        self.name = row["upwork_name"]
        self.fileid = row["fileid"]
        self.crntEvent = row["eventname"]
        self.fxn = lambda x: 1 if x == "play" or x=="submit" else 0
        self.state= self.fxn(self.crntEvent)
        self.events = {}
        self.total = row["duration"]
        self.idle = 0
        self.events["checkout"] = 0
        self.events["observation"] = 0
        self.events["pause"] = 0
        self.events["play"] = 0
        self.events["position"] = 0
        self.events["speed"] = 0
        self.events["submit"] = 0
        self.events["submitcancel"] = 0
        self.events["timer"] = 0
        self.events[self.crntEvent]+=row["duration"]
        if not self.state and row["duration"]>5:
            self.idle += row["duration"]

    def newSegment(self, row):
        if row["duration"]==None:
            row["duration"] = 0
        self.crntEvent = row["eventname"]
        self.date = row['date']
        self.state= self.fxn(self.crntEvent)
        self.total += row["duration"]
        self.events[self.crntEvent]+=row["duration"]
        if not self.state and row["duration"]>5:
            self.idle += row["duration"]

    

def openDB(fyle="ace.json"):
    cfg=None
    try:
            with open(fyle, "r") as json_file:
                cfg = json.load(json_file)
            sql="""
                        SELECT 
                        ot.observerid as "observerid", 
                        o.upwork_name,
                        va.fileid,
                        DATE(ot.dt_startwalltime_utc) AS "date",
                        ot.dt_startwalltime_utc AS "walltime",
                        ROUND( ( LEAD(ot.startwalltime,1) OVER (PARTITION BY o.id, va.fileid ORDER BY ot.dt_startwalltime_utc)  - ot.startwalltime), 1) AS "duration",
                        coalesce (LEAD(ot.dt_startwalltime_utc,1) OVER (PARTITION BY o.id, va.fileid ORDER BY ot.dt_startwalltime_utc), 0) AS next_event_walltime,
                        ot.eventname

                        FROM gtx.observer_timings ot
                        JOIN gtx.observer o ON o.id = ot.observerid
                        JOIN gtx.video_action va ON va.id = checkinid
                        WHERE o.upwork_name IS NOT NULL
                        ORDER BY ot.observerid, va.fileid, ot.dt_startwalltime_utc"""

            db_conn = pymysql.connect(host=cfg["dbserver"], user=cfg["dbuser"], passwd= cfg["dbpassword"], db="gtx", connect_timeout=35)
            db_cur = db_conn.cursor(pymysql.cursors.DictCursor)
            print("Database Accessed")
            db_cur.execute(sql)
            return db_conn, db_cur, db_cur.fetchall()

    except Exception as e:
            print("Database Connection Failed ==> ERROR: %s" % str(e))
            sys.exit(1)

def writeDB(db_conn, db_cur, entry):
    sql = """ INSERT INTO gtx.observers_efficiency_qa (observerid, upwork_name, fileid, date_worked, secs_idle, secs_checkout, secs_observation, secs_pause, secs_play, secs_position, secs_speed, secs_submit, secs_submitcancel, secs_timer, secs_total) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    values = (entry.oid, entry.name, entry.fileid, entry.date, entry.idle, entry.events["checkout"], entry.events["observation"], entry.events["pause"], entry.events["play"], entry.events["position"], entry.events["speed"], entry.events["submit"], entry.events["submitcancel"], entry.events["timer"], entry.total)
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
            required=True,
            help='Relative or fully qualified path to the .JSON configuration file to use',
        )

        return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    fyle= args.config
    [db_conn,db_cur,query]=openDB(fyle)
    obs = {}

    prev = None
    crnt = None
    x=0
    for row in query:
        if row["observerid"] not in obs.keys() or not obs[row["observerid"]] == row["fileid"]:
            prev = crnt
            crnt=obsTime(row)
            obs[row["observerid"]] = row["fileid"]
            x+=1
            if x>1:
                writeDB(db_conn, db_cur, prev)
                print(prev.name)

        else:
            crnt.newSegment(row)
            
    
    closeDB(db_conn, db_cur)