import sys
import argparse
import json
import pymysql

class obsTime():
    def __init__(self,row):
        if row["duration"]==None:
            row["duration"] = 0
        self.oid = row["observerid"]
        self.start =row['date']
        self.date = row['date']
        self.name = row["name"]
        self.fileid = row["fileid"]
        self.totalTime = row["duration"]
        self.event = row["eventname"]
        self.idle = 0
        self.speed = "0"
        self.play = 0
        self.whatSpeed()
        self.isPlay()
        self.half = 0
        self.one = 0
        self.two = 0
        self.three = 0
        self.four = 0
        self.five = 0
        self.totalPlayTime = 0
        self.events = {}
        self.events["checkout"] = 0
        self.events["observation"] = 0
        self.events["pause"] = 0
        self.events["submit"] = 0
        self.events["submitcancel"] = 0
        self.events["timer"] = 0
        self.events["play 0.5"] = 0
        self.events["play 1"] = 0
        self.events["play 2"] = 0
        self.events["play 3"] = 0
        self.events["play 4"] = 0
        self.events["play 5"] = 0
        self.events["speed 0.5"] = 0
        self.events["speed 1"] = 0
        self.events["speed 2"] = 0
        self.events["speed 3"] = 0
        self.events["speed 4"] = 0
        self.events["speed 5"] = 0
        self.events["autoplaySpeed 0.5"] = 0
        self.events["autoplaySpeed 1"] = 0
        self.events["autoplaySpeed 2"] = 0
        self.events["autoplaySpeed 3"] = 0
        self.events["autoplaySpeed 4"] = 0
        self.events["autoplaySpeed 5"] = 0
        self.events["goToPrevObservation"] = 0
        self.events["goToNextObservation"] = 0
        self.events["backToWork"] = 0
        self.events["deleteObservation"] = 0
        self.events["agreeObservation"] = 0
        self.events["position 0.0"] = 0
        self.events["position -0.0"] = 0
        self.events["position 0.5"] = 0
        self.events["position -0.5"] = 0
        self.events["position 10.0"] = 0
        self.events["position -10.0"] = 0

        self.events[self.event]+=row["duration"]

        if self.play and not self.event[0:4]=="play":
            self.events["play " + self.speed]+=row["duration"]
        if not self.play and row["duration"]>5:
            self.idle += row["duration"]


    def isPlay(self):
        if self.event[:4] == "play" or self.event[:4] == "auto":
            self.play = 1
        elif self.event[:5] == "pause" or self.event[:5] == "check" or self.event == "submit":
            self.play = 0

    def whatSpeed(self):
        if self.event[:4] == "play":
            self.speed = self.event[5:]
        elif self.event[:4] == "auto":
            self.speed = self.event[14:]
        if self.event[:5] == "speed":
            self.speed = self.event[6:]

    def newSegment(self, row):
        if row["duration"]==None:
            row["duration"] = 0
        self.event = row["eventname"]
        self.date = row['date']
        self.whatSpeed()
        self.isPlay()
        if not self.event == "submit":
            self.totalTime += row["duration"]
            self.events[self.event]+=row["duration"]
            if self.play and not self.event[0:4]=="play":
                self.events["play " + self.speed]+=row["duration"]
            if not self.play and row["duration"]>5:
                self.idle += row["duration"]

    def endSegment(self):
        self.half = self.events["play 0.5"]/self.totalTime
        self.one = self.events["play 1"]/self.totalTime
        self.two = self.events["play 2"]/self.totalTime
        self.three = self.events["play 3"]/self.totalTime
        self.four = self.events["play 4"]/self.totalTime
        self.five = self.events["play 5"]/self.totalTime
        self.totalPlayTime = self.events["play 0.5"] + self.events["play 1"] + self.events["play 2"] + self.events["play 3"] + self.events["play 4"] + self.events["play 5"]
        self.playRatio = self.totalPlayTime/self.totalTime
        self.idleRatio = self.idle/self.totalTime


def openDB(fyle):
    cfg=None
    try:
            with open(fyle, "r") as json_file:
                cfg = json.load(json_file)
            sql="""
                        SELECT 
                        ot.observerid AS "observerid", 
                        o.fullname AS "name",
                        va.fileid,
                        (ot.dt_endwalltime_utc) AS "date",
                        ot.dt_endwalltime_utc AS "walltime",
                        ( ( LEAD(ot.endwalltime,1) OVER (PARTITION BY o.id, va.fileid ORDER BY ot.endwalltime)  - ot.endwalltime)) AS "duration",
                        coalesce (LEAD(ot.dt_endwalltime_utc,1) OVER (PARTITION BY o.id, va.fileid ORDER BY ot.endwalltime), 0) AS next_event_walltime,
                        ot.eventname
                        FROM gtx.observer_timings ot
                        JOIN gtx.observer o ON o.id = ot.observerid
                        JOIN gtx.video_action va ON va.id = checkinid
                        WHERE (dt_endwalltime_utc) > "2022-08-02 13:00:00" -- all entries after time specified follow updated format
                        ORDER BY ot.observerid, va.fileid, ot.endwalltime"""

            db_conn = pymysql.connect(host=cfg["dbserver"], user=cfg["dbuser"], passwd= cfg["dbpassword"], db="gtx", connect_timeout=35)
            db_cur = db_conn.cursor(pymysql.cursors.DictCursor)
            print("Database Accessed")
            db_cur.execute(sql)
            return db_conn, db_cur, db_cur.fetchall()

    except Exception as e:
            print("Database Connection Failed ==> ERROR: %s" % str(e))
            sys.exit(1)

def writeDB(db_conn, db_cur, entry):
    sql = """ INSERT INTO gtx.observer_timings_analysis_qa (observerid, name, fileid, date_worked_utc, dt_start_utc, dt_end_utc, total_duration_seconds, play_duration_seconds, idle_seconds, half_play_seconds, one_play_seconds, two_play_seconds, three_play_seconds, four_play_seconds, five_play_seconds, half_autoplay_seconds, one_autoplay_seconds, two_autoplay_seconds, three_autoplay_seconds, four_autoplay_seconds, five_autoplay_seconds, half_speed_seconds, one_speed_seconds, two_speed_seconds, three_speed_seconds, four_speed_seconds, five_speed_seconds, checkout_seconds, observation_seconds, goToPrevObservation_seconds, goToNextObservation_seconds, agreeObservation_seconds, deleteObservation_seconds, backToWork_seconds, pause_seconds, frame_rewind_seconds, half_rewind_seconds, ten_rewind_seconds, frame_forward_seconds, half_forward_seconds, ten_forward_seconds, submit_seconds, submitcancel_seconds, timer_seconds, play_duration_to_total, idle_duration_to_total, half_speed_to_play_duration, one_speed_to_play_duration, two_speed_to_play_duration, three_speed_to_play_duration, four_speed_to_play_duration, five_speed_to_play_duration)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    values =                                    (entry.oid, entry.name, entry.fileid, entry.date, entry.start, entry.date, entry.totalTime, entry.totalPlayTime, entry.idle, entry.events["play 0.5"], entry.events["play 1"], entry.events["play 2"], entry.events["play 3"], entry.events["play 4"], entry.events["play 5"], entry.events["autoplaySpeed 0.5"], entry.events["autoplaySpeed 1"], entry.events["autoplaySpeed 2"], entry.events["autoplaySpeed 3"], entry.events["autoplaySpeed 4"], entry.events["autoplaySpeed 5"], entry.events["speed 0.5"], entry.events["speed 1"], entry.events["speed 2"], entry.events["speed 3"], entry.events["speed 4"], entry.events["speed 5"], entry.events["checkout"], entry.events["observation"], entry.events["goToPrevObservation"], entry.events["goToNextObservation"], entry.events['agreeObservation'], entry.events['deleteObservation'], entry.events["backToWork"], entry.events['pause'], entry.events['position -0.0'], entry.events['position -0.5'], entry.events['position 10.0'], entry.events['position 0.0'], entry.events['position 0.5'], entry.events['position 10.0'], entry.events["submit"], entry.events['submitcancel'], entry.events["timer"], entry.playRatio, entry.idleRatio, entry.half, entry.one, entry.two, entry.three, entry.four, entry.five) 
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
                prev.endSegment()
                print(prev.fileid,"->", prev.totalTime,prev.totalPlayTime, prev.playRatio ,prev.idleRatio, prev.idle)
                writeDB(db_conn, db_cur, prev)

        else:
            crnt.newSegment(row)
            
    closeDB(db_conn, db_cur)