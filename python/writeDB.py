""" 
Code adapted format of Alan's sample.py 
Author: Ace Abdulrahman
Stand alone program that writes to motion_frames table
"""

import os
import sys
import argparse
import json
import pymysql

from time import sleep

class gtxDB:                          
    def __init__(self,fyle=None):

        print ('All Systems Ready\n*** Initiating Process ***')
        self.jsonData = None
        self.cfg = None
        self.vidName = None

        self.args = self._parse_args()
        self.dir= self.args.dir
        if self.dir:
            self.jsonData = self.readJson(fyle)
            self.cfg = self.readJson(self.args.config)
            self.vidName = fyle[:-5]

        else:
            self.jsonData = self.readJson(self.args.jsonFrames)
            self.cfg = self.readJson(self.args.config)
            self.vidName = self.args.jsonFrames[:-5]

        self.fileID = None
        self.db_conn = None
        self.db_cur = None
        self.eventnbr = None

        self.first_frame = None
        self.first_bbox_l = None 
        self.first_bbox_t = None
        self.first_bbox_r = None
        self.first_bbox_b = None
        self.first_time_in_video = None

        self.best_frame = None
        self.best_bbox_l = None 
        self.best_bbox_t = None
        self.best_bbox_r = None
        self.best_bbox_b = None
        self.best_time_in_video = None

        self.last_frame = None
        self.last_bbox_l = None 
        self.last_bbox_t = None
        self.last_bbox_r = None
        self.last_bbox_b = None
        self.last_time_in_video = None

    def readJson(self, fyle):
        data = None

        try:
            with open(fyle, "r") as json_file:
                data = json.load(json_file)

        except Exception as e:
            print("Unable to Load File ==> ERROR: %s" % str(e))

        return data

    def openDB(self):
        try:
            self.db_conn = pymysql.connect(host=self.cfg["dbserver"], user=self.cfg["dbuser"], passwd= self.cfg["dbpassword"], db="gtx", connect_timeout=35)
            self.db_cur = self.db_conn.cursor(pymysql.cursors.DictCursor)
            print("Database Accessed")

        except Exception as e:
            print("Database Connection Failed ==> ERROR: %s" % str(e))
            sys.exit(1)

    def closeDB(self):
        self.db_conn.commit()
        self.db_conn.close()
        print("Database Closed\n*** Mission Passed: Respect + ***")
    
    def parseJson(self):
        for eventnbr in self.jsonData:

            # ENTRIES FOR FIRST BEST LAST FRAMES
            self.eventnbr = eventnbr
            first = self.jsonData[eventnbr]["first"]
            best = self.jsonData[eventnbr]["best"]
            last = self.jsonData[eventnbr]["last"]

            # FIRST FRAME FIELDS
            self.first_frame = first["frame_number"]

            first_bbox = first["bbox"]
            self.first_bbox_l = first_bbox[0] 
            self.first_bbox_t = first_bbox[1]
            self.first_bbox_r = first_bbox[2]
            self.first_bbox_b = first_bbox[3]

            self.first_time_in_video = first["time_within_video"]

            # BEST FRAME FIELDS
            self.best_frame = best["frame_number"]

            best_bbox = best["bbox"]
            self.best_bbox_l = best_bbox[0] 
            self.best_bbox_t = best_bbox[1]
            self.best_bbox_r = best_bbox[2]
            self.best_bbox_b = best_bbox[3]

            self.best_time_in_video = best["time_within_video"]

            # LAST FRAME FIELDS
            self.last_frame = last["frame_number"]

            last_bbox = last["bbox"]
            self.last_bbox_l = last_bbox[0] 
            self.last_bbox_t = last_bbox[1]
            self.last_bbox_r = last_bbox[2]
            self.last_bbox_b = last_bbox[3]

            self.last_time_in_video = last["time_within_video"]

            self.findFileID()
            self.writeDB()
        print("All Entries Inserted")

    def findFileID(self):
        sql = "SELECT id FROM gtx.video WHERE basename = (%s)"              
        try:
            self.db_cur.execute(sql, (self.vidName))
            self.fileID = self.db_cur.fetchone()["id"]

        except Exception as e:
            print("Database search failed ==> ERROR: %s" % str(e))
            sys.exit(1)
    
    def writeDB(self):
        label= "ace testing"
        stage = "qa"
        sql = """INSERT INTO gtx.motion_frames (fileID, stage, label, 
                                                first_frame, first_bbox_l, first_bbox_t, first_bbox_r, first_bbox_b, first_time_in_video, 
                                                best_frame, best_bbox_l, best_bbox_t, best_bbox_r, best_bbox_b, best_time_in_video, 
                                                last_frame, last_bbox_l, last_bbox_t, last_bbox_r, last_bbox_b, last_time_in_video,
                                                eventnbr) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""  # ¯\_(ツ)_/¯ 
        try:
            self.db_cur.execute(sql, (self.fileID, stage, label, 
                                self.first_frame, self.first_bbox_l, self.first_bbox_t, self.first_bbox_r, self.first_bbox_b, self.first_time_in_video, 
                                self.best_frame, self.best_bbox_l, self.best_bbox_t, self.best_bbox_r, self.best_bbox_b, self.best_time_in_video, 
                                self.last_frame, self.last_bbox_l, self.last_bbox_t, self.last_bbox_r, self.last_bbox_b, self.last_time_in_video,
                                self.eventnbr))
        except Exception as e:
            print("Database insertion failed ==>ERROR: %s" % str(e))
            sys.exit(1)

    def _parse_args(self):
        parser = argparse.ArgumentParser(description='Ingestor', 
                                        formatter_class=argparse.RawDescriptionHelpFormatter)

        parser.add_argument(
            '--config',
            dest='config',
            required=True,
            help='Relative or fully qualified path to the .JSON configuration file to use'
        )
        group = parser.add_mutually_exclusive_group(required=True)
       
        group.add_argument(
            '--jsonFrames',
            dest='jsonFrames',
            help='Relative or fully qualified path to the .JSON gtx motion file to use'
        )

        group.add_argument(
            '--dir',
            dest='dir',
            help='Relative or fully qualified path to the directory containing .JSON gtx motion files to use'
        )

        return parser.parse_args()

    def run(self):
        self.openDB()
        self.parseJson()
        self.findFileID()
        self.closeDB()

if __name__ == "__main__":
    gtxEntry = gtxDB()
    if gtxEntry.dir:
        gtxFiles = [gtxVid for gtxVid in os.listdir(gtxEntry.dir) if gtxVid.endswith(".json") and gtxVid.startswith("gtx")]
        print(gtxFiles)  
        sleep(3)    
    else:
        gtxEntry.run()