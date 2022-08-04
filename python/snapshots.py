import sys
import os
import argparse
import json
import pymysql

class gtxDB:                          
    def __init__(self,fyle):

        print ('All Systems Ready\n*** Initiating Process ***')
        self.date_worked = None

        self.args = self._parse_args()
        self.cfg = self.readJson(self.args.config)
        self.query = self.readJson(fyle)


        self.db_conn = None
        self.db_cur = None
        self.row_count = 0

        self.server_time = None
        self.first_name = None
        self.last_name = None
        self.timezone = None
        self.timezone_offset = None
        # self.minutes_time = None                            #Timestamp
        # self.minutes_mouse = None                           #Clicks
        # self.minutes_keyboard = None                        #Events
        self.time= None                                     #Timestamp
        self.status = None                                  #Status
        self.contractId = None                              #Contract 
        self.mouseEventsCount = None                        #CountMouse 
        self.keyboardEventsCount = None                     #Keyboard 
        self.uid = None                                     #UID
        self.activeWindowTitle = None                       #Window 
        self.teamName = None                                #Team 
        self.taskid= None                                   #Task
        self.taskdescription= None                          #Task
        self.memo = None                                    #Memo
        self.minute_instances = None

        self.time_1 = None
        self.mouse_1 = None
        self.keyboard_1 = None
        self.time_2 = None
        self.mouse_2 = None
        self.keyboard_2 = None
        self.time_3 = None
        self.mouse_3 = None
        self.keyboard_3 = None
        self.time_4 = None
        self.mouse_4 = None
        self.keyboard_4 = None
        self.time_5 = None
        self.mouse_5 = None
        self.keyboard_5 = None
        self.time_6 = None
        self.mouse_6 = None
        self.keyboard_6 = None
        self.time_7 = None
        self.mouse_7 = None
        self.keyboard_7 = None
        self.time_8 = None
        self.mouse_8 = None
        self.keyboard_8 = None
        self.time_9 = None
        self.mouse_9 = None
        self.keyboard_9 = None
        self.time_10 = None
        self.mouse_10 = None
        self.keyboard_10 = None
        self.time_11 = None
        self.mouse_11 = None
        self.keyboard_11 = None
        self.time_12 = None
        self.mouse_12 = None
        self.keyboard_12 = None
        self.time_13 = None
        self.mouse_13 = None
        self.keyboard_13 = None
        self.time_14 = None
        self.mouse_14 = None
        self.keyboard_14 = None
        self.time_15 = None
        self.mouse_15 = None
        self.keyboard_15 = None
        self.time_16 = None
        self.mouse_16 = None
        self.keyboard_16 = None
        self.time_17 = None
        self.mouse_17 = None
        self.keyboard_17 = None
        self.time_18 = None
        self.mouse_18 = None
        self.keyboard_18 = None
        self.time_19 = None
        self.mouse_19 = None
        self.keyboard_19 = None
        self.time_20 = None
        self.mouse_20 = None
        self.keyboard_20 = None
        self.time_21 = None
        self.mouse_21 = None
        self.keyboard_21 = None
        self.time_22 = None
        self.mouse_22 = None
        self.keyboard_22 = None
        self.time_23 = None
        self.mouse_23 = None
        self.keyboard_23 = None
        self.time_24 = None
        self.mouse_24 = None
        self.keyboard_24 = None
        self.time_25 = None
        self.mouse_25 = None
        self.keyboard_25 = None
        self.time_26 = None
        self.mouse_26 = None
        self.keyboard_26 = None
        self.time_27 = None
        self.mouse_27 = None
        self.keyboard_27 = None
        self.last_minute_time= None
        
        self.hasScreenshot= None                            #Flag indicates if the screenshot exists
        self.screenshotURL= None                            #UrlScreenshot 
        self.screenshotImg= None                            #Screenshot 
        self.screenshotImgLrg= None                         #Large 
        self.screenshotImgMed = None                        #Medium 
        self.screenshotImgThmb = None                       #Screenshot 
        self.hasCamerashot = None                           #Flag indicates if a camera screenshot available
        self.webcamUrl = None                               #Webcam URL
        self.webcamImg = None                               #Webcam image 
        self.webcamImgThmb = None                           #Webcam image thumbnail
        self.companyId = None                               #Company ID
        self.workdiary_api = None                           #URL to Workdiary API

    def reset(self):
        
        self.time= None                                     #Timestamp
        self.status = None                                  #Status
        self.contractId = None                              #Contract 
        self.mouseEventsCount = None                        #CountMouse 
        self.keyboardEventsCount = None                     #Keyboard 
        self.uid = None                                     #UID
        self.activeWindowTitle = None                       #Window 
        self.teamName = None                                #Team 
        self.taskid= None                                   #Task
        self.taskdescription= None                          #Task
        self.memo = None                                    #Memo
        self.minute_instances = None

        self.time_1 = None
        self.mouse_1 = None
        self.keyboard_1 = None
        self.time_2 = None
        self.mouse_2 = None
        self.keyboard_2 = None
        self.time_3 = None
        self.mouse_3 = None
        self.keyboard_3 = None
        self.time_4 = None
        self.mouse_4 = None
        self.keyboard_4 = None
        self.time_5 = None
        self.mouse_5 = None
        self.keyboard_5 = None
        self.time_6 = None
        self.mouse_6 = None
        self.keyboard_6 = None
        self.time_7 = None
        self.mouse_7 = None
        self.keyboard_7 = None
        self.time_8 = None
        self.mouse_8 = None
        self.keyboard_8 = None
        self.time_9 = None
        self.mouse_9 = None
        self.keyboard_9 = None
        self.time_10 = None
        self.mouse_10 = None
        self.keyboard_10 = None
        self.time_11 = None
        self.mouse_11 = None
        self.keyboard_11 = None
        self.time_12 = None
        self.mouse_12 = None
        self.keyboard_12 = None
        self.time_13 = None
        self.mouse_13 = None
        self.keyboard_13 = None
        self.time_14 = None
        self.mouse_14 = None
        self.keyboard_14 = None
        self.time_15 = None
        self.mouse_15 = None
        self.keyboard_15 = None
        self.time_16 = None
        self.mouse_16 = None
        self.keyboard_16 = None
        self.time_17 = None
        self.mouse_17 = None
        self.keyboard_17 = None
        self.time_18 = None
        self.mouse_18 = None
        self.keyboard_18 = None
        self.time_19 = None
        self.mouse_19 = None
        self.keyboard_19 = None
        self.time_20 = None
        self.mouse_20 = None
        self.keyboard_20 = None
        self.time_21 = None
        self.mouse_21 = None
        self.keyboard_21 = None
        self.time_22 = None
        self.mouse_22 = None
        self.keyboard_22 = None
        self.time_23 = None
        self.mouse_23 = None
        self.keyboard_23 = None
        self.time_24 = None
        self.mouse_24 = None
        self.keyboard_24 = None
        self.time_25 = None
        self.mouse_25 = None
        self.keyboard_25 = None
        self.time_26 = None
        self.mouse_26 = None
        self.keyboard_26 = None
        self.time_27 = None
        self.mouse_27 = None
        self.keyboard_27 = None
        self.last_minute_time= None
        
        self.hasScreenshot= None                            #Flag indicates if the screenshot exists
        self.screenshotURL= None                            #UrlScreenshot 
        self.screenshotImg= None                            #Screenshot 
        self.screenshotImgLrg= None                         #Large 
        self.screenshotImgMed = None                        #Medium 
        self.screenshotImgThmb = None                       #Screenshot 
        self.hasCamerashot = None                           #Flag indicates if a camera screenshot available
        self.webcamUrl = None                               #Webcam URL
        self.webcamImg = None                               #Webcam image 
        self.webcamImgThmb = None                           #Webcam image thumbnail
        self.companyId = None                               #Company ID
        self.workdiary_api = None                           #URL to Workdiary API

    def readJson(self, f):
        data = None

        try:
            with open(f, "r") as json_file:
                data = json.load(json_file)
                self.date_worked = f[-13:-9]+ "-" + f[-9:-7]+ "-" +f[-7:-5]

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
    
    def parseQuery(self):
        for snapshot in self.query:
            self.minute_instances = len(snapshot["minutes"])
            # snapshot['minutes'].sort(key = lambda x : x['time'] )
            self.last_minute_time = snapshot['minutes'][len(snapshot["minutes"])-1]["time"]
            print(snapshot['minutes'])
            

            self.time = snapshot["time"]                                     #Timestamp
            self.status = snapshot["status"]                                  #Status
            self.contractId = snapshot["contractId"]                              #Contract 
            self.mouseEventsCount = snapshot["mouseEventsCount"]                        #CountMouse 
            self.keyboardEventsCount = snapshot["keyboardEventsCount"]                   #Keyboard 
            self.uid = snapshot ["uid"]                                    #UID
            self.activeWindowTitle = snapshot["activeWindowTitle"]                       #Window 
            self.teamName = snapshot["teamName"]

            if snapshot["task"]:                               #Team 
                self.taskid = snapshot["task"] ["id"]                                   #Task
                self.taskdescription = snapshot["task"] ["description"]                          #Task

            self.memo = snapshot["memo"]                                    #Memo
            if len(snapshot["minutes"])>0 :
                self.time_1 = snapshot["minutes"][0]["time"]
                self.mouse_1 = snapshot["minutes"][0]["mouse"]
                self.keyboard_1 = snapshot["minutes"][0]["keyboard"]

            if len(snapshot["minutes"])>1:
                self.time_2 = snapshot["minutes"][1]["time"]
                self.mouse_2 = snapshot["minutes"][1]["mouse"]
                self.keyboard_2 = snapshot["minutes"][1]["keyboard"]

            if len(snapshot["minutes"])>2:
                self.time_3 = snapshot["minutes"][2]["time"]
                self.mouse_3 = snapshot["minutes"][2]["mouse"]
                self.keyboard_3 = snapshot["minutes"][2]["keyboard"]
            
            if len(snapshot["minutes"])>3:
                self.time_4 = snapshot["minutes"][3]["time"]
                self.mouse_4 = snapshot["minutes"][3]["mouse"]
                self.keyboard_4 = snapshot["minutes"][3]["keyboard"]

            if len(snapshot["minutes"])>4:
                self.time_5 = snapshot["minutes"][4]["time"]
                self.mouse_5 = snapshot["minutes"][4]["mouse"]
                self.keyboard_5 = snapshot["minutes"][4]["keyboard"]

            if len(snapshot["minutes"])>5:
                self.time_6 = snapshot["minutes"][5]["time"]
                self.mouse_6 = snapshot["minutes"][5]["mouse"]
                self.keyboard_6 = snapshot["minutes"][5]["keyboard"]
            if len(snapshot["minutes"])>6:
                self.time_7 = snapshot["minutes"][6]["time"]
                self.mouse_7 = snapshot["minutes"][6]["mouse"]
                self.keyboard_7 = snapshot["minutes"][6]["keyboard"]

            if len(snapshot["minutes"])>7:
                self.time_8 = snapshot["minutes"][7]["time"]
                self.mouse_8 = snapshot["minutes"][7]["mouse"]
                self.keyboard_8 = snapshot["minutes"][7]["keyboard"]
            if len(snapshot["minutes"])>8:
                self.time_9 = snapshot["minutes"][8]["time"]
                self.mouse_9 = snapshot["minutes"][8]["mouse"]
                self.keyboard_9 = snapshot["minutes"][8]["keyboard"]
            if len(snapshot["minutes"])>9:
                self.time_10 = snapshot["minutes"][9]["time"]
                self.mouse_10 = snapshot["minutes"][9]["mouse"]
                self.keyboard_10 = snapshot["minutes"][9]["keyboard"]
            if len(snapshot["minutes"])>10:
                self.time_11 = snapshot["minutes"][10]["time"]
                self.mouse_11 = snapshot["minutes"][10]["mouse"]
                self.keyboard_11 = snapshot["minutes"][10]["keyboard"]
            if len(snapshot["minutes"])>11:
                            self.time_12 = snapshot["minutes"][11]["time"]
                            self.mouse_12 = snapshot["minutes"][11]["mouse"]
                            self.keyboard_12 = snapshot["minutes"][11]["keyboard"]
            if len(snapshot["minutes"])>12:
                            self.time_13 = snapshot["minutes"][12]["time"]
                            self.mouse_13 = snapshot["minutes"][12]["mouse"]
                            self.keyboard_13 = snapshot["minutes"][12]["keyboard"]
            if len(snapshot["minutes"])>13:
                            self.time_14 = snapshot["minutes"][13]["time"]
                            self.mouse_14 = snapshot["minutes"][13]["mouse"]
                            self.keyboard_14 = snapshot["minutes"][13]["keyboard"]
            if len(snapshot["minutes"])>14:
                            self.time_15 = snapshot["minutes"][14]["time"]
                            self.mouse_15 = snapshot["minutes"][14]["mouse"]
                            self.keyboard_15 = snapshot["minutes"][14]["keyboard"]
            if len(snapshot["minutes"])>15:
                            self.time_16 = snapshot["minutes"][15]["time"]
                            self.mouse_16 = snapshot["minutes"][15]["mouse"]
                            self.keyboard_16 = snapshot["minutes"][15]["keyboard"]
            if len(snapshot["minutes"])>16:
                            self.time_17 = snapshot["minutes"][16]["time"]
                            self.mouse_17 = snapshot["minutes"][16]["mouse"]
                            self.keyboard_17 = snapshot["minutes"][16]["keyboard"]
            if len(snapshot["minutes"])>17:
                            self.time_18 = snapshot["minutes"][17]["time"]
                            self.mouse_18 = snapshot["minutes"][17]["mouse"]
                            self.keyboard_18 = snapshot["minutes"][17]["keyboard"]
            if len(snapshot["minutes"])>18:
                            self.time_19 = snapshot["minutes"][18]["time"]
                            self.mouse_19 = snapshot["minutes"][18]["mouse"]
                            self.keyboard_19 = snapshot["minutes"][18]["keyboard"]
            if len(snapshot["minutes"])>19:
                            self.time_20 = snapshot["minutes"][19]["time"]
                            self.mouse_20 = snapshot["minutes"][19]["mouse"]
                            self.keyboard_20 = snapshot["minutes"][19]["keyboard"]
            if len(snapshot["minutes"])>20:
                            self.time_21 = snapshot["minutes"][20]["time"]
                            self.mouse_21 = snapshot["minutes"][20]["mouse"]
                            self.keyboard_21 = snapshot["minutes"][20]["keyboard"]
            if len(snapshot["minutes"])>21:
                            self.time_22 = snapshot["minutes"][21]["time"]
                            self.mouse_22 = snapshot["minutes"][21]["mouse"]
                            self.keyboard_22 = snapshot["minutes"][21]["keyboard"]
            if len(snapshot["minutes"])>22:
                            self.time_23 = snapshot["minutes"][22]["time"]
                            self.mouse_23 = snapshot["minutes"][22]["mouse"]
                            self.keyboard_23 = snapshot["minutes"][22]["keyboard"]
            if len(snapshot["minutes"])>23:
                            self.time_24 = snapshot["minutes"][23]["time"]
                            self.mouse_24 = snapshot["minutes"][23]["mouse"]
                            self.keyboard_24 = snapshot["minutes"][23]["keyboard"]
            if len(snapshot["minutes"])>24:
                            self.time_25 = snapshot["minutes"][24]["time"]
                            self.mouse_25 = snapshot["minutes"][24]["mouse"]
                            self.keyboard_25 = snapshot["minutes"][24]["keyboard"]
            if len(snapshot["minutes"])>25:
                            self.time_26 = snapshot["minutes"][25]["time"]
                            self.mouse_26 = snapshot["minutes"][25]["mouse"]
                            self.keyboard_26 = snapshot["minutes"][25]["keyboard"]
            if len(snapshot["minutes"])>26:
                            self.time_27 = snapshot["minutes"][26]["time"]
                            self.mouse_27 = snapshot["minutes"][26]["mouse"]
                            self.keyboard_27 = snapshot["minutes"][26]["keyboard"]

            self.hasScreenshot = snapshot ["hasScreenshot"]                           #Flag indicates if the screenshot exists
            self.screenshotURL = snapshot ["screenshotUrl"]                            #UrlScreenshot 
            self.screenshotImg = snapshot ["screenshotImg"]                           #Screenshot 
            self.screenshotImgLrg = snapshot ["screenshotImgLrg"]                         #Large 
            self.screenshotImgMed = snapshot ["screenshotImgMed"]                      #Medium 
            self.screenshotImgThmb = snapshot ["screenshotImgThmb"]                      #Screenshot 
            self.hasCamerashot = snapshot ["hasCamerashot"]                          #Flag indicates if a camera screenshot available
            self.webcamUrl = snapshot ["webcamUrl"]                              #Webcam URL
            self.webcamImg = snapshot ["webcamImg"]                              #Webcam image 
            self.webcamImgThmb = snapshot ["webcamImgThmb"]                          #Webcam image thumbnail
            self.companyId = snapshot ["companyId"]                               #Company ID
            self.workdiary_api = snapshot["workdiary_api"]                          #URL to Workdiary API

            self.writeDB()
            self.reset()
        print(f" {self.row_count} rows inserted")


    def writeDB(self):
        label= "ace testing"
        stage = "qa"
        sql_insert = """INSERT INTO gtx.snapshots_qa (time, status, contractId, mouseEventsCount, keyboardEventsCount, uid, activeWindowTitle, teamName, taskId, description, memo, minute_instances, minute1_time, minute1_mouse, minute1_keyboard, minute2_time, minute2_mouse, minute2_keyboard, minute3_time, minute3_mouse, minute3_keyboard, minute4_time, minute4_mouse, minute4_keyboard, minute5_time, minute5_mouse, minute5_keyboard, minute6_time, minute6_mouse, minute6_keyboard, minute7_time, minute7_mouse, minute7_keyboard, minute8_time, minute8_mouse, minute8_keyboard, minute9_time, minute9_mouse, minute9_keyboard, minute10_time, minute10_mouse, minute10_keyboard, minute11_time, minute11_mouse, minute11_keyboard, minute12_time, minute12_mouse, minute12_keyboard, minute13_time, minute13_mouse, minute13_keyboard, minute14_time, minute14_mouse, minute14_keyboard, minute15_time, minute15_mouse, minute15_keyboard, minute16_time, minute16_mouse, minute16_keyboard, minute17_time, minute17_mouse, minute17_keyboard, minute18_time, minute18_mouse, minute18_keyboard, minute19_time, minute19_mouse, minute19_keyboard, minute20_time, minute20_mouse, minute20_keyboard, minute21_time, minute21_mouse, minute21_keyboard, minute22_time, minute22_mouse, minute22_keyboard, minute23_time, minute23_mouse, minute23_keyboard, minute24_time, minute24_mouse, minute24_keyboard, minute25_time, minute25_mouse, minute25_keyboard, minute26_time, minute26_mouse, minute26_keyboard, minute27_time, minute27_mouse, minute27_keyboard, last_minute_time, hasScreenshot, screenshotUrl, screenshotImg, screenshotImgLrg, screenshotImgMed, screenshotImgThmb, hasCameraShot, webcamUrl, webcamImg, webcamImgThmb, companyId, workdiary_api, date_worked) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.row_count+=1
        try:
            self.db_cur.execute(sql_insert, (   self.time,
                                                self.status,
                                                self.contractId,
                                                self.mouseEventsCount,
                                                self.keyboardEventsCount,
                                                self.uid,
                                                self.activeWindowTitle,
                                                self.teamName,
                                                self.taskid,
                                                self.taskdescription,
                                                self.memo,
                                                self.minute_instances,
                                                self.time_1,
                                                self.mouse_1,
                                                self.keyboard_1,
                                                self.time_2,
                                                self.mouse_2,
                                                self.keyboard_2,
                                                self.time_3,
                                                self.mouse_3,
                                                self.keyboard_3,
                                                self.time_4,
                                                self.mouse_4,
                                                self.keyboard_4,
                                                self.time_5,
                                                self.mouse_5,
                                                self.keyboard_5,
                                                self.time_6,
                                                self.mouse_6,
                                                self.keyboard_6,
                                                self.time_7,
                                                self.mouse_7,
                                                self.keyboard_7,
                                                self.time_8,
                                                self.mouse_8, 
                                                self.keyboard_8,
                                                self.time_9,
                                                self.mouse_9,
                                                self.keyboard_9,
                                                self.time_10,
                                                self.mouse_10,
                                                self.keyboard_10,
                                                self.time_11,
                                                self.mouse_11,
                                                self.keyboard_11,
                                                self.time_12,
                                                self.mouse_12,
                                                self.keyboard_12,
                                                self.time_13,
                                                self.mouse_13,
                                                self.keyboard_13,
                                                self.time_14,
                                                self.mouse_14,
                                                self.keyboard_14,
                                                self.time_15,
                                                self.mouse_15,
                                                self.keyboard_15,
                                                self.time_16,
                                                self.mouse_16,
                                                self.keyboard_16,
                                                self.time_17,
                                                self.mouse_17,
                                                self.keyboard_17,
                                                self.time_18,
                                                self.mouse_18,
                                                self.keyboard_18,
                                                self.time_19,
                                                self.mouse_19,
                                                self.keyboard_19,
                                                self.time_20,
                                                self.mouse_20,
                                                self.keyboard_20,
                                                self.time_21,
                                                self.mouse_21,
                                                self.keyboard_21,
                                                self.time_22,
                                                self.mouse_22,
                                                self.keyboard_22,
                                                self.time_23,
                                                self.mouse_23,
                                                self.keyboard_23,
                                                self.time_24,
                                                self.mouse_24,
                                                self.keyboard_24,
                                                self.time_25,
                                                self.mouse_25,
                                                self.keyboard_25,
                                                self.time_26,
                                                self.mouse_26,
                                                self.keyboard_26,
                                                self.time_27,
                                                self.mouse_27,
                                                self.keyboard_27,
                                                self.last_minute_time,
                                                self.hasScreenshot,
                                                self.screenshotURL,
                                                self.screenshotImg,
                                                self.screenshotImgLrg,
                                                self.screenshotImgMed,
                                                self.screenshotImgThmb,
                                                self.hasCamerashot,
                                                self.webcamUrl,
                                                self.webcamImg,
                                                self.webcamImgThmb,
                                                self.companyId,
                                                self.workdiary_api,
                                                self.date_worked
                                                ))
            

        except Exception as e:
            print("Database insertion failed ==> ERROR: %s" % str(e))
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
        
        return parser.parse_args()

    def run(self):
        self.openDB()
        self.parseQuery()
        self.closeDB()
        

if __name__ == "__main__":
    directory = input("Enter directory that contains json snapshots: ")
    for fyle in os.listdir(directory):
        gtxEntry = gtxDB(directory+fyle)
        print(gtxEntry.date_worked + "\n")
        gtxEntry.run()
        
    




