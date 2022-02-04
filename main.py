"""
    This is a program meant to run on a Raspberry Pi which is meant to collect data for OEE calculations.

    The program will sit at the main screen and collect counts of good parts and bad parts. It will synchronize this data
    with the server using MQTT. The server will send down a list of products and users that will be selectable on the main
    screen. These are not required though in case the operators forget to enter this information. This can be filled in later
    on the website.

    When a stop signal is detected, the program will switch to the "Downtime Reasons" screen. The user will have the option
    to select the reson why the machine stopped and (if required) clarify with a secondary reason. Then the program will
    wait at the "Thank You" screen until the maching starts running again. Note that the program will transition to the main
    screen at any point that the machine starts running. If the user didn't enter a downtime reason, they will be able to do
    so on the website at a later time.
"""

from enum import IntEnum
import os
import datetime, pytz
from time import localtime
import paho.mqtt.client as paho
import ssl
from kivy.app import App
from kivy.clock import Clock
from kivy.config import Config
from kivy.properties import StringProperty, NumericProperty, BooleanProperty, ObjectProperty
from kivymd.app import MDApp
from kivymd.uix.button import MDRectangleFlatButton, MDFlatButton, MDRaisedButton
from kivymd.uix.list import OneLineAvatarIconListItem
from kivymd.uix.screen import MDScreen
from kivymd.uix.boxlayout import MDBoxLayout
from kivymd.uix.dialog import MDDialog
from polarisdb import PolarisDb
from polarisdb import PolarisLogType
from configparser import ConfigParser, NoOptionError, NoSectionError
import requests
import json
import uuid
try:
    import RPi.GPIO as GPIO # type: ignore
except ImportError:
    # We're not running on a Raspberri Pi, but continue anyway so we can test on the dev machine
    print('Failed to import Raspberry Pi dependencies')
except:
    pass

class RunningState(IntEnum):
    UNKNOWN = -1
    STOPPED = 0
    RUNNING = 1
    MINOR_STOP = 2

class ItemConfirm(OneLineAvatarIconListItem):
    divider = None

    def set_icon(self, instance_check):
        instance_check.active = True
        check_list = instance_check.get_widgets(instance_check.group)
        for check in check_list:
            if check != instance_check:
                check.active = False

class DowntimeReasonButton(MDRaisedButton):
    def __init__(self, reasonId, isPrimary, **kwargs):
        super(DowntimeReasonButton, self).__init__(**kwargs)

        self.reasonId = reasonId
        self.isPrimary = isPrimary

class CountPerHourContent(MDBoxLayout):
    pass

class MainScreen(MDScreen):
    pass

class SystemStopScreen(MDScreen):
    pass

class DowntimeScreen(MDScreen):
    pass

class AnalysisScreen(MDScreen):
    pass

class PerceptivePolaris(MDBoxLayout):
    pass

class PolarisApp(MDApp):

    # Device Settings
    serverUrl = '' 
    serverApiKey = '' 
    deviceName = StringProperty('')
    deviceLocation = StringProperty('')
    deviceSerialNumber = StringProperty('')

    initializedWithDatabase = False
    initializedWithServer = False
    initialized = False

    # MQTT Settings
    brokerHost = '' # redacted
    brokerPort = 8883
    mqttclient = paho.Client()
    teamId = '' # redacted

    # Diagnostics
    uptimeSeconds = 0

    # Update Rates
    updateRateSeconds = 0.5
    syncRateSeconds = 3
    gpioUpdateRateSeconds = 0.1

    # Display Properties
    screenTitleDisplay = StringProperty('OPERATOR MAIN SCREEN')
    productNameDisplay = StringProperty('<Product>')
    goalCountDisplay = StringProperty('0')
    operatorNameDisplay = StringProperty('<Operator>')
    goodCountDisplay = StringProperty('0')
    rejectCountDisplay = StringProperty('0')
    uptimeDisplay = StringProperty('00:00:00')
    downtimeDisplay = StringProperty('00:00:00')
    downtimeReasonDisplay = StringProperty('<DOWNTIME REASON>')

    selectedProductId = 0
    selectedGoalCph = 0
    goalCountValue = StringProperty('0') # This is used for the number pad
    selectedOperatorId = 0
    goodCountValue = 0
    rejectCountValue = 0
    minorStopDurationSecs = 0

    runningTagId = 0
    goodCountTagId = 0
    rejectCountTagId = 0

    productDialog = None
    countPerHourDialog = None
    operatorDialog = None
    exitDialog = None

    previousRunningState = RunningState.UNKNOWN
    currentRunningState = NumericProperty(0)

    tagDataThatNeedsReason = None
    selectedPrimaryDowntimeReason = 0

    # Downtime Variables
    downtimeSecondaryReasonsVisible = BooleanProperty(False)

    # Raspberry Pi Inputs
    isRaspberryPi = False

    inputPin1 = 23
    inputPin2 = 24
    inputPin3 = 25

    previousInput1 = False
    previousInput2 = False
    previousInput3 = False

    
    def build(self):

        db = PolarisDb()
        db.createTables()


        self.loadSettings()
        self.initialize()
        self.initializeWithServer()

        Clock.schedule_interval(self.update, self.updateRateSeconds)
        Clock.schedule_interval(self.syncWithServer, self.syncRateSeconds)

        if self.isRaspberryPi:
            Clock.schedule_interval(self.updateGPIO, self.gpioUpdateRateSeconds)

        return PerceptivePolaris()

    def initialize(self):

        osUName = os.uname()[4]
        self.isRaspberryPi = osUName.startswith('arm')

        if self.isRaspberryPi:       
            GPIO.setmode(GPIO.BCM) 
            GPIO.setup(self.inputPin1, GPIO.IN)
            GPIO.setup(self.inputPin2, GPIO.IN)
            GPIO.setup(self.inputPin3, GPIO.IN)

        self.mqttclient.on_message=self.on_mqtt_message
        self.mqttclient.tls_set(tls_version=ssl.PROTOCOL_TLSv1_2)
        self.mqttclient.tls_insecure_set(True) # MQTT server doesn't have 

        self.initializedWithDatabase = True

        self.addLogEntry(PolarisLogType.STATUS, "Program started")

    def loadSettings(self):
        config = ConfigParser()
        config.read('config.ini')

        try:
            self.serverUrl = config.get('device', 'ServerURL')
            self.serverApiKey = config.get('device', 'APIKey')
            self.deviceName = config.get('device', 'Name')
            self.deviceLocation = config.get('device', 'Location')
            self.deviceSerialNumber = config.get('device', 'SerialNumber')
        except NoOptionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Device parameters not set')
        except NoSectionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Device section not defined in config file')
        
        db = PolarisDb()
        goalPerHour = 0
        try:
            self.selectedProductId = config.getint('main', 'SelectedProductId')
            tmpProduct = db.getProductFromId(self.selectedProductId)
            self.productNameDisplay = tmpProduct['name'] if tmpProduct is not None else "<Product>"
            goalPerHour = float(tmpProduct['ideal_cph']) if tmpProduct is not None else 0
        except NoOptionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Selected Product ID not in config file')
        except NoSectionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Main section not defined in config file')

        try:
            # Override the saved value if it's in the settings
            tmpGoalPerHour = config.getfloat('main', 'GoalCph')
            goalPerHour = tmpGoalPerHour if tmpGoalPerHour != 0 else goalPerHour
        except NoOptionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Goal Count Per Hour not in config file')
        except NoSectionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Section not defined in config file')

        self.goalCountDisplay = f"{goalPerHour} per Hour"
        self.selectedGoalCph = goalPerHour
        

        try:
            self.selectedOperatorId = config.getint('main', 'SelectedOperatorId')
        except NoOptionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Selected Operator ID not in config file')
        except NoSectionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Section not defined in config file')
        

        try:
            self.minorStopDurationSecs = config.getint('main', 'MinorStopDurationSecs')
        except NoOptionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Minor Stop Duration not in config file')
        except NoSectionError:
            self.addLogEntry(PolarisLogType.ERROR, 'Section not defined in config file')

    def initializeWithServer(self):
        d = {
            'datetime': datetime.datetime.now(),
            'resync': True
        }
        self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_request/resync", json.dumps(d, default=str))


    
    def saveSettings(self):

        config = ConfigParser()

        config.add_section('device')
        config.set('device', 'ServerURL', self.serverUrl or '<Server URL>')
        config.set('device', 'APIKey', self.serverApiKey or '<API Key>')
        config.set('device', 'Name', self.deviceName or '<Name>')
        config.set('device', 'Location', self.deviceLocation or '<Location>')
        config.set('device', 'SerialNumber', self.deviceSerialNumber or '<SerialNum>')

        config.add_section('main')
        config.set('main', 'SelectedProductId', str(self.selectedProductId))
        config.set('main', 'GoalCph', str(self.goalCountValue))
        config.set('main', 'SelectedOperatorId', str(self.selectedOperatorId))
        config.set('main', 'MinorStopDurationSecs', str(self.minorStopDurationSecs))

        with open('config.ini', 'w') as f:
            config.write(f)


    def exitApp(self):
        """Displays a dialog box that allows the user to exit the application"""

        if not self.exitDialog:
            self.exitDialog = MDDialog(
                text="Exit?",
                buttons=[
                    MDFlatButton(
                        text="CANCEL", text_color=self.theme_cls.primary_color, on_release=lambda x: (self.exitDialog.dismiss())
                    ),
                    MDFlatButton(
                        text="OK", text_color=self.theme_cls.primary_color, on_release=lambda x: (App.get_running_app().stop())
                    ),
                ],
            )
        self.exitDialog.open()
        #App.get_running_app().stop()

    def showProductSelection(self):

        db = PolarisDb()
        products = db.getProducts()

        productList = [
            ItemConfirm(text=product['name'], on_press=lambda x, p=product['id']: self.setSelectedProductId(p)) for product in products
        ]

        if not self.productDialog:
            self.productDialog = MDDialog(
                title="Select Product",
                type="confirmation",
                items=productList,
                buttons=[
                    MDFlatButton(
                        text="CANCEL", text_color=self.theme_cls.primary_color, on_release=self.closeProductDialog
                    ),
                    MDRectangleFlatButton(
                        text="OK", text_color=self.theme_cls.primary_color, on_release=self.setProduct
                    ),
                ],
            )
        self.productDialog.open()

    def showCountPerHourSelector(self):
        
        if not self.countPerHourDialog:
            self.countPerHourDialog = MDDialog(
                title="Select Ideal Count per Hour",
                type="custom",
                content_cls=CountPerHourContent(),
                buttons=[
                    MDFlatButton(
                        text="CANCEL", text_color=self.theme_cls.primary_color, on_release=self.closeCountPerHourDialog
                    ),
                    MDRectangleFlatButton(
                        text="OK", text_color=self.theme_cls.primary_color, on_release=self.setCountPerHour
                    ),
                ],
            )
        self.countPerHourDialog.open()

    def showOperatorSelection(self):

        db = PolarisDb()
        users = db.getUsers()

        operatorList = [
            ItemConfirm(text="%s %s" % (user['fname'], user['lname']), on_press=lambda x, u=user['id']: self.setSelectedOperatorId(u)) for user in users
        ]

        if not self.operatorDialog:
            self.operatorDialog = MDDialog(
                title="Select Operator",
                type="confirmation",
                items=operatorList,
                buttons=[
                    MDFlatButton(
                        text="CANCEL", text_color=self.theme_cls.primary_color, on_release=self.closeOperatorDialog
                    ),
                    MDRectangleFlatButton(
                        text="OK", text_color=self.theme_cls.primary_color, on_release=self.setOperator
                    ),
                ],
            )
        self.operatorDialog.open()

    def setSelectedProductId(self, id):
        self.selectedProductId = id
        self.addLogEntry(PolarisLogType.STATUS, f"Product Id: {self.selectedProductId}")
        self.goalCountValue = '0'
        self.setCountPerHour()

    def setSelectedOperatorId(self, id):
        self.selectedOperatorId = id
        self.addLogEntry(PolarisLogType.STATUS, f"Operator Id: {self.selectedOperatorId}")

    def setProduct(self, inst):
        db = PolarisDb()
        self.productNameDisplay = db.getProductFromId(self.selectedProductId)['name']
        self.saveSettings()
        self.productDialog.dismiss()

    def closeProductDialog(self, inst):
        self.productDialog.dismiss()

    def setCountPerHour(self, inst=None):
        if self.goalCountValue == '0':
            db = PolarisDb()
            tmpProduct = db.getProductFromId(self.selectedProductId)
            self.goalCountValue = "{:.1f}".format(tmpProduct['ideal_cph']) if tmpProduct else self.goalCountValue
        self.goalCountDisplay = self.goalCountValue
        self.selectedGoalCph = float(self.goalCountValue)
        self.saveSettings()
        if inst is not None:
            self.countPerHourDialog.dismiss()

    def closeCountPerHourDialog(self, inst):
        self.countPerHourDialog.dismiss()

    def setOperator(self, inst):
        self.saveSettings()
        self.operatorDialog.dismiss()

    def closeOperatorDialog(self, inst):
        self.operatorDialog.dismiss()

    def keypad(self, num):
        """Add the digit to the value that will be displayed"""
        self.goalCountValue = "%s%s" % ("" if self.goalCountValue == "0" else self.goalCountValue, num)

    def keypadDecimal(self):
        """Only add a decimal if one doesn't already exist"""
        if '.' not in self.goalCountValue:
            self.goalCountValue = "%s%s" % (self.goalCountValue, '.')

    def keypadBackspace(self):
        """Remove the last character, but show a zero if there are no more characters left"""
        self.goalCountValue = str(self.goalCountValue)[:-1]
        self.goalCountValue = self.goalCountValue if self.goalCountValue != "" else "0"

    def setGoodCountPerHour(self):
        self.goodCountDisplay = f"{round(self.goodCountValue, 2)}/hour"

    def setRejectCountPerHour(self):
        self.rejectCountDisplay = f"{round(self.rejectCountValue, 2)}/hour"

    def setUptimeMins(self, uptimeMins):
        self.uptimeDisplay = str(datetime.timedelta(minutes=uptimeMins)).split('.', 2)[0]

    def setDowntimeMins(self, downtimeMins):
        self.downtimeDisplay = str(datetime.timedelta(minutes=downtimeMins)).split('.', 2)[0]

    def downtimePrimaryReasonSelected(self, downtimeReasonBtn):
        db = PolarisDb()

        reasonId = downtimeReasonBtn.reasonId

        secondaryDowntimeReasons = db.getSecondaryDowntimeReasons(reasonId)

        if downtimeReasonBtn.isPrimary:

            if len(secondaryDowntimeReasons) == 0:
                self.saveDowntimeReasonToDatabase(reasonId)
            else:

                self.root.ids.sm.screens[1].ids.secondaryReasonWidget.clear_widgets()

                for reason in secondaryDowntimeReasons:
                    btn = DowntimeReasonButton(
                            reasonId=reason['id'],
                            isPrimary=False,
                            text=reason['name'], 
                            size_hint=[1.0,0.2],
                            height = 15
                        )
                    btn.bind(on_press=self.downtimePrimaryReasonSelected)
                    self.root.ids.sm.screens[1].ids.secondaryReasonWidget.add_widget(btn)
        else:
            self.saveDowntimeReasonToDatabase(reasonId)

    def saveDowntimeReasonToDatabase(self, downtimeReasonId):
        db = PolarisDb()
        self.addLogEntry(PolarisLogType.STATUS, "Primary Selected Reason Num: %s" % downtimeReasonId)
        
        db.addDowntimeReasonForTag(self.tagDataThatNeedsReason['tag_id'], downtimeReasonId)

        self.setDowntimeReason()
        self.navigateToDowntime()


    def setDowntimeReason(self):
        db = PolarisDb()

        result = db.getDowntimeReasonsForTag(self.runningTagId)

        if len(result) > 0:
            self.downtimeReasonDisplay = result[-1]['downtime_reason']


    def navigateToMain(self):
        self.root.ids.sm.current = 'main'
        self.screenTitleDisplay = 'OPERATOR MAIN SCREEN'
    
    def navigateToSystemStop(self):
        self.root.ids.sm.current = 'system_stop'
        self.screenTitleDisplay = 'SYSTEM STOP SCREEN'


        db = PolarisDb()

        primaryDowntimeReasons = db.getPrimaryDowntimeReasons()

        for reason in primaryDowntimeReasons:
            btn = DowntimeReasonButton(
                    reasonId=reason['id'],
                    isPrimary=True,
                    text=reason['name'], 
                    size_hint=[1.0,0.2],
                    height = 15
                )
            
            btn.bind(on_press=self.downtimePrimaryReasonSelected)
            self.root.ids.sm.screens[1].ids.primaryReasonWidget.add_widget(btn)

        self.root.ids.sm.screens[1].ids.secondaryReasonWidget.clear_widgets()

    def navigateToDowntime(self):
        self.downtimeSecondaryReasonsVisible = False
        self.root.ids.sm.current = 'downtime'
        self.screenTitleDisplay = 'MACHINE IN DOWNTIME'

    def navigateToAnalysis(self):
        self.root.ids.sm.current = 'analysis'
        self.screenTitleDisplay = 'ANALYSIS SCREEN'

    def update(self, dt):
        self.uptimeSeconds += dt

        #print(f"Uptime Seconds: {self.uptimeSeconds}")

        db = PolarisDb()

        # make sure that we have the tag ids set
        if self.runningTagId == 0:
            runningTag = db.getRunningTag()
            self.runningTagId = runningTag['id'] if runningTag else 0
        else:
            runningTag = db.getTagFromId(self.runningTagId)
            runningData = db.getLastTagDataForTagId(runningTag['id'])

            self.currentRunningState = runningData['int_value'] if runningData is not None else 0
            #TODO: The running state is "stopped", need to check if there is a reason

            if self.currentRunningState == int(RunningState.RUNNING):
                self.setUptimeMins(self.uptimeSeconds / 60)
            else:
                self.setDowntimeMins(self.uptimeSeconds / 60)
                self.setDowntimeReason()


            #TODO: This only checks for downtime entries that need an assigned reason if the state changes
            #   If there is more than one stop, we need to assign a value for each one
            #   Also, the downtime reason screen should identify which stop we're referring to
            if self.currentRunningState != self.previousRunningState:

                if self.currentRunningState == int(RunningState.STOPPED):

                    tagDatasThatNeedReason = db.getRunningStatusWithoutDowntimeReason(runningTag['id'])

                    # running state is stopped and there's no reason set for it
                    if len(tagDatasThatNeedReason)> 0:
                        self.tagDataThatNeedsReason = tagDatasThatNeedReason[0]
                        self.navigateToSystemStop()
                    else:
                        self.navigateToMain()
                    #self.navigateToAnalysis()
                else:
                    self.navigateToMain()

            self.previousRunningState = self.currentRunningState

        if self.goodCountTagId == 0:
            goodCountTag = db.getTagFromName('Good')
            self.goodCountTagId = goodCountTag['id'] if goodCountTag else 0
        else:
            goodCount = db.getTagDataHourCountForTagId(self.goodCountTagId)
            previousGoodCount = self.goodCountValue
            self.goodCountValue = goodCount['count'] if goodCount else 0
            if previousGoodCount != self.goodCountValue:
                self.setGoodCountPerHour()



        if self.rejectCountTagId == 0:
            rejectCountTag = db.getTagFromName('Reject')
            self.rejectCountTagId = rejectCountTag['id'] if rejectCountTag else 0
        else:
            rejectCount = db.getTagDataHourCountForTagId(self.rejectCountTagId)
            previousRejectCount = self.rejectCountValue
            self.rejectCountValue = rejectCount['count'] if rejectCount else 0
            if previousRejectCount != self.rejectCountValue:
                self.setRejectCountPerHour()


    def updateGPIO(self, dt):
        """Read the Raspberry Pi GPIO pins and save the values to the database"""

        db = PolarisDb()

        currentInput1 = GPIO.input(self.inputPin1)
        currentInput2 = GPIO.input(self.inputPin2)
        currentInput3 = GPIO.input(self.inputPin3)
        
        if currentInput1 != self.previousInput1:
            db.addTagIntData(self.runningTagId, self.selectedProductId, currentInput1)

        if currentInput2 and currentInput2 != self.previousInput2:
            db.addTagIntData(self.goodCountTagId, self.selectedProductId, currentInput2)

        if currentInput3 and currentInput3 != self.previousInput3:
            db.addTagIntData(self.rejectCountTagId, self.selectedProductId, currentInput3)
        
        
        self.previousInput1 = currentInput1
        self.previousInput2 = currentInput2
        self.previousInput3 = currentInput3
        

    def on_mqtt_message(self, client, userdata, message):
        #self.addLogEntry(PolarisLogType.STATUS, f'received message: {message.payload.decode("utf-8")}, topic: {message.topic}, qos: {message.qos}')

        topicParts = message.topic.split('/')
        data = message.payload.decode("utf-8")

        """
            <team_id>/<device_serial_number>/tagdata/request
            topicParts[0] - Team ID
            topicParts[1] - Device Serial Number
            topicParts[2] - Either client_request/client_response or server_request/server_response
            topicParts[3] - What we're referring to
        """

        if len(topicParts) > 3:
            if topicParts[2] == 'server_request':
                # This means that we've received data or a request from the server

                if topicParts[3] == 'device':
                    # The server request contains our name and location
                    try:
                        deviceSettings = json.loads(data)

                        self.deviceName = deviceSettings['name']
                        self.deviceLocation = deviceSettings['location']
                        self.saveSettings()

                        #<team_id>/<device_serial_number>/request/device

                        newUUID = str(uuid.uuid4())
                        now = datetime.datetime.now()

                        d = {
                            'sync_id': newUUID,
                            'updated_at': now
                        }

                        # Let the server know that we received the data
                        self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/device", json.dumps(d, default=str))
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing device data from MQTT: {e}")

                elif topicParts[3] == 'downtimereason':
                    # The server is sending us the options for a downtime reason
                    try:
                        downtimeReason = json.loads(data)

                        db = PolarisDb()

                        if downtimeReason['sync_id']:
                            # See if we already have a copy with the sync id
                            tmpDTR = db.getDowntimeReasonFromSyncId(downtimeReason['sync_id'])
                            if tmpDTR is not None:
                                # Get the sync id for the id that is referenced
                                if 'is_secondary_for_sync_id' in downtimeReason:
                                    tmpPrimaryDTR = db.getDowntimeReasonFromSyncId(downtimeReason['is_secondary_for_sync_id'])
                                    # If this is the secondary but we can't find the primary, then most likely the primary
                                    # hasn't been synchronized yet. Just return for now and hopefully it synchronizes later
                                    if tmpPrimaryDTR is None: return

                                    db.updateDowntimeReasonsBySyncId(
                                        sync_id=downtimeReason['sync_id'],
                                        name=downtimeReason['name'],
                                        is_secondary_for=tmpPrimaryDTR['id'],
                                        deleted_at=downtimeReason['deleted_at'])
                                else:
                                    db.updateDowntimeReasonsBySyncId(
                                        sync_id=downtimeReason['sync_id'],
                                        name=downtimeReason['name'],
                                        deleted_at=downtimeReason['deleted_at'])

                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/downtimereason", json.dumps(downtimeReason, default=str))
                            else:
                                # We received a tag without a sync id, but we don't have it yet
                                if downtimeReason['is_secondary_for'] is None:
                                    result = db.addPrimaryDowntimeReason(downtimeReason['name'], downtimeReason['sync_id'])
                                else:
                                    if downtimeReason['is_secondary_for_sync_id'] is not None:
                                        tmpPrimaryDTR = db.getDowntimeReasonFromSyncId(downtimeReason['is_secondary_for_sync_id'])
                                        if tmpPrimaryDTR is None: return
                                        result = db.addSecondaryDowntimeReason(downtimeReason['name'], tmpPrimaryDTR['id'], downtimeReason['sync_id'])
                                #result = db.addTag(tag['name'], tag['description'], tag['is_running_signal'], tag['type'], tag['sync_id'])
                                
                                # Send the downtime reason to the server which will contain the sync id
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/downtimereason", json.dumps(downtimeReason, default=str))
                                pass
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing Downtime Reasons from MQTT: {e}")

                elif topicParts[3] == 'product':
                    # The server is sending us a copy of the available products
                    try:
                        product = json.loads(data)

                        db = PolarisDb()

                        if product['sync_id']:
                            # See if we already have a copy with the sync id
                            tmpProduct = db.getProductFromSyncId(product['sync_id'])
                            if tmpProduct is not None:
                                db.updateProductBySyncId(
                                    sync_id=product['sync_id'],
                                    name=product['name'],
                                    productCode=product['product_code'],
                                    ideal_cph=product['ideal_cph'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/product", json.dumps(product, default=str))
                            else:
                                # We received a product with a sync id, but we don't have it yet
                                result = db.addProduct(product['name'], product['product_code'], product['ideal_cph'], product['sync_id'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/product", json.dumps(product, default=str))

                            # If we're running the product that was just updated, update the display now

                            tmpProduct = db.getProductFromSyncId(product['sync_id'])
                            if self.selectedProductId == tmpProduct['id']:
                                self.goalCountValue = '0'
                                self.setCountPerHour()
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing Product from MQTT: {e}")

                elif topicParts[3] == 'tag':
                    # The server is sending us the available tag data
                    try:
                        tag = json.loads(data)

                        db = PolarisDb()

                        if tag['sync_id']:
                            # See if we already have a copy with the sync id
                            tmpTag = db.getTagFromSyncId(tag['sync_id'])
                            if tmpTag is not None:
                                db.updateTagBySyncId(
                                    sync_id=tag['sync_id'],
                                    name=tag['name'],
                                    description=tag['description'],
                                    isRunningSignal=tag['is_running_signal'],
                                    type=tag['type'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/tag", json.dumps(tag, default=str))
                            else:
                                # We received a tag with a sync id, but we don't have it yet
                                result = db.addTag(tag['name'], tag['description'], tag['is_running_signal'], tag['type'], tag['sync_id'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/tag", json.dumps(tag, default=str))
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing Tag from MQTT: {e}")

                elif topicParts[3] == 'user':
                    # The server is sending us a list of the available users
                    try:
                        user = json.loads(data)

                        db = PolarisDb()

                        if user['sync_id']:
                            # See if we already have a copy with the sync id
                            tmpUser = db.getUserFromSyncId(user['sync_id'])
                            if tmpUser is not None:
                                db.updateUserBySyncId(
                                    sync_id=user['sync_id'],
                                    fname=user['fname'],
                                    lname=user['lname'],
                                    is_device_admin=user['is_device_admin'],
                                    is_device_operator=user['is_device_operator'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/user", json.dumps(user, default=str))
                            else:
                                # We received a user with a sync id, but we don't have it yet
                                result = db.addUser(user['fname'], user['lname'], user['is_device_admin'], user['is_device_operator'], user['sync_id'])
                                self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_response/user", json.dumps(user, default=str))
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing User from MQTT: {e}")

            elif topicParts[2] == 'server_response':
                # We sent the server a request and it is sending a response

                if topicParts[3] == 'tagdata':
                    # The server received our tag data and we're going to save the sync id so we know not to send it again
                    try:
                        tagData = json.loads(data)

                        db = PolarisDb()

                        db.updateTagDataSyncId(tagData['id'], tagData['sync_id'])

                        self.addLogEntry(PolarisLogType.STATUS, f"Received synced tag data from server - Id: {tagData['id']}, Sync Id: {tagData['sync_id']}")
                        #if needToSaveUUID:
                        #    db.updateTagDataSyncId(d['id'], newUUID)
                        #else:
                        #    db.updateTagDataNeedsResync(d['id'])
                    except Exception as e:
                        self.addLogEntry(PolarisLogType.ERROR, f"Error parsing Tag Data response from server: {e}")
                        
                elif topicParts[3] == 'heartbeat':
                    # For testing. Remove for production so we don't fill the database with heartbeat responses
                    self.addLogEntry(PolarisLogType.STATUS, "Heartbeat")
                    pass





    def syncWithServer(self, dt):

        now = datetime.datetime.now()

        d = {
            'datetime': now
        }

        self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_request/heartbeat", json.dumps(d, default=str))
        
        
        # The following code has been commented out for now so we don't try to flood the server with unnecessary info
        # Start by checking the device settings
        #try:
        #    self.syncDeviceData()
        #except Exception as e:
        #    self.addLogEntry(PolarisLogType.ERROR, f"Error syncing device data with server: {e}")

        #try:
        #    self.syncTagData()
        #except Exception as e:
        #    self.addLogEntry(PolarisLogType.ERROR, f"Error syncing tag data with server: {e}")
        #pass
        
    
    # This should no longer be needed because we're just going to wait for a signal from the server that the data has changed
    def syncDeviceData(self):

        url = f"{self.serverUrl}/api/device/{self.deviceSerialNumber}?api_token={self.serverApiKey}"
        r = requests.get(url)
        data = r.json()

        #webURL = urllib.request.urlopen(f"{self.serverUrl}/api/device/{self.deviceSerialNumber}?api_token={self.serverApiKey}")
        #data = webURL.read()
        #encoding = webURL.info().get_content_charset('utf-8')
        #JSON_object = json.loads(data.decode(encoding))

        if len(data) == 0:
            self.addLogEntry(PolarisLogType.ERROR, 'Serial number not set!')
        elif 'serial_num' in data:
            self.deviceName = data['name']
            self.deviceLocation = data['location']
            self.minorStopDurationSecs = data['minor_stop_duration_secs']

            self.saveSettings()

        #webURL.close()

    def syncTagData(self):
        db = PolarisDb()

        data = db.getTagDataNotSynced(25)

        if not data: return

        for d in data:

            # Update the timezone to eastern so that we can convert it to UTC
            eastern = pytz.timezone('US/Eastern')
            localCreatedAt = eastern.localize(d['created_at'])
            d['created_at'] = eastern.normalize(localCreatedAt).astimezone(pytz.utc)

            tmpTag = db.getTagFromId(d['tag_id'])
            d['tag_sync_id'] = tmpTag['sync_id']

            tmpProduct = db.getProductFromId(d['product_id'])
            d['product_sync_id'] = tmpProduct['sync_id']

            if d['downtime_reason_id'] is not None:
                tmpDTR = db.getDowntimeReasonFromId(d['downtime_reason_id'])
                d['downtime_reason_sync_id'] = tmpDTR['sync_id']

            #d['sync_id'] = str(uuid.uuid4())
            #db.updateTagDataSyncId(d['id'], d['sync_id'])


            self.mqttclient.publish(f"{self.teamId}/{self.deviceSerialNumber}/client_request/tagdata", json.dumps(d, default=str))


    def checkSyncedTagData(self):
        pass

    
    def addLogEntry(self, type, message):

        now = datetime.datetime.now()

        if type == PolarisLogType.STATUS:
            print(f"{now} STATUS: {message}")
        elif type == PolarisLogType.ERROR:
            print(f"{now} ERROR: {message}")
        
        db = PolarisDb()
        db.addLogEntry(type, message)

        

if __name__ == '__main__':

    if os.uname()[4].startswith('arm'):
        # If we're running on the Raspberry Pi, we want to be full screen
        Config.set('graphics', 'borderless', True)
        Config.set('graphics', 'fullscreen', False)
        Config.set('graphics', 'window_state', 'maximized')
        Config.write()
    else:
        Config.set('graphics', 'borderless', False)
        Config.set('graphics', 'fullscreen', False)
        Config.set('graphics', 'window_state', 'visible')
        Config.write()
    #Window.fullscreen = 'auto'


    PolarisApp().run()
