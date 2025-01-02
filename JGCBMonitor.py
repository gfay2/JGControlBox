#!/usr/bin/env python3

"""
Master JG Control Box Status Monitor

The purpose of this application is to:
1) Establish communication with redundant GS4 motor drivers using BACnet for control and communication
2) Establish communication with an Arduino-Mego to receive analog sensor data over UART serial
3) Establish communication with Raspberry-Pi-2 using XXXX to detect if it's online and it's state
4) Report data from both GS4 drivers and sensors to MQTT broker
5) Track hour usage of motors driven by GS4
6) Update Arduino-Mega code over USB serial

"""

import sys
import time
import syslog

import paho.mqtt.client as mqtt

from datetime import datetime

from collections import deque

from bacpypes.debugging import bacpypes_debugging, ModuleLogger
from bacpypes.consolelogging import ConfigArgumentParser

from bacpypes.core import run, stop, deferred
from bacpypes.iocb import IOCB
from bacpypes.task import RecurringTask

from bacpypes.pdu import Address
from bacpypes.object import get_datatype

from bacpypes.apdu import ReadPropertyRequest, ReadPropertyACK
from bacpypes.primitivedata import Unsigned, ObjectIdentifier
from bacpypes.constructeddata import Array

# from bacpypes.app import BIPSimpleApplication
from bacpypes.local.device import LocalDeviceObject

from misty.mstplib import MSTPSimpleApplication

from JGCBMonitor_config import mqtt_broker_address, mqtt_broker_port, mqtt_JGCB_topics, mqtt_keep_alive_time
from JGCBMonitor_config import mqtt_first_reconnect_delay, mqtt_reconnect_rate, mqtt_max_reconnect_delay
from JGCBMonitor_config import JGCB_mqtt_ID, JGCB_interval
from JGCBMonitor_config import GS4_point_list

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# globals
mqtt_connected = False

#
#  Recurring Task to Monitor GS4 Drive System
#  Read each GS4 drive parameter from static point_list using BACnet protocol
#

@bacpypes_debugging
class PrairieDog(MSTPSimpleApplication, RecurringTask):
    
    def __init__(self, interval, client, *args):
        if _debug: PrairieDog._debug("__init__ %r %r", interval, args)
        MSTPSimpleApplication.__init__(self, *args)
        # set interval of recurring task (in seconds)
        RecurringTask.__init__(self, interval * 1000)

        # initialize to not busy
        self.GS4_busy = False
        self.arduino1_busy = False
        self.mqtt_client = client
        
        # install the task
        self.install_task()
        if _debug: PrairieDog._debug("    - install_task")

    def process_task(self):
        if _debug: PrairieDog._debug("process_task")

        # check to see if task is idle
        if self.is_busy():
            return

        # now we are busy
        self.GS4_busy = True
        #self.arduino1_busy = True

        # turn the point list into a queue
        self.point_queue = deque(GS4_point_list)
        if _debug: PrairieDog._debug("    - deque point list %r", self.point_queue)

        # clear out response value list
        self.response_values = []

        # read next GS4 data
        self.next_GS4_request()

        # read next Arduino sensor outputs
        # TODO self.next_arduino_request()

    def next_GS4_request(self):
        if _debug: PrairieDog._debug("next_GS4_request")

        # check to see if the point queue is empty
        if not self.point_queue:
            # if so, complete GS4 read
            self.read_GS4_complete()

            return

        # get the next GS4 reuest
        addr, obj_id, prop_id = self.point_queue.popleft()
        obj_id = ObjectIdentifier(obj_id).value

        datatype = get_datatype(obj_id[0], prop_id)
        if not datatype:
            raise ValueError("invalid property for object type")

        # build a request
        request = ReadPropertyRequest(
            objectIdentifier=obj_id,
            propertyIdentifier=prop_id,
            )
        request.pduDestination = Address(addr)
        if _debug: PrairieDog._debug("    - request: %r", request)

        # make an IOCB
        iocb = IOCB(request)
        if _debug: PrairieDog._debug("    - iocb: %r", iocb)

        # set a callback for the request
        iocb.add_callback(self.GS4_request_callback)
        if _debug: PrairieDog._debug("    - GS4 request callback set")

        # give the iocb to the application
        self.request_io(iocb)

    def GS4_request_callback(self, iocb):
        if _debug: PrairieDog._debug("GS4_request_callback %r", iocb)

        # do something for error/reject/abort
        if iocb.ioError:
            sys.stdout.write(str(iocb.ioError) + '\n')
            if _debug: PrairieDog._debug("    - error: %r", iocb.ioError)
            self.response_values.append(iocb.ioError)

        # iocb successful
        elif iocb.ioResponse:
            apdu = iocb.ioResponse

            # should be an ack
            if not isinstance(apdu, ReadPropertyACK):
                if _debug: PrairieDog._debug("    - not an ack")
                stop()

            # find the datatype
            datatype = get_datatype(apdu.objectIdentifier[0], apdu.propertyIdentifier)
            if _debug: PrairieDog._debug("    - datatype: %r", datatype)
            if not datatype:
                raise TypeError("unknown datatype")

            # special case for array parts, others are managed by cast_out
            if issubclass(datatype, Array) and (apdu.propertyArrayIndex is not None):
                if apdu.propertyArrayIndex == 0:
                    value = apdu.propertyValue.cast_out(Unsigned)
                else:
                    value = apdu.propertyValue.cast_out(datatype.subtype)
            else:
                value = apdu.propertyValue.cast_out(datatype)
            if _debug: PrairieDog._debug("    - value: %r", value)

            # write each value to stdout as it is received for debugging
            #sys.stdout.write(str(value) + '\n')
            #if hasattr(value, 'debug_contents'):
                #value.debug_contents(file=sys.stdout)
            #sys.stdout.flush()

            # save the value
            self.response_values.append(value)

        # no ioError or ioResponse
        else:
            if _debug: PrairieDog._debug._debug("    - GS4 ioError or ioResponse expected")

        # queue the next GS4 request
        deferred(self.next_GS4_request)

    def read_GS4_complete(self):
        global mqtt_connected
        if _debug: PrairieDog._debug("read_GS4_complete")

        # dump out the request and results to screen for debug
        #for request, response in zip(GS4_point_list, self.response_values):
        #    print(request, response)

        # Publish results to JGCB mqtt topic
        idx = 0
        for topic in mqtt_JGCB_topics:
            if mqtt_connected == True:
                self.mqtt_client.publish(self.response_values[idx])
                self.mqtt_client.publish(self.response_values[idx+1])
                self.mqtt_client.publish(self.response_values[idx+2])
                self.mqtt_client.publish(self.response_values[idx+3])

            print(self.response_values[idx], self.response_values[idx+1]), print(self.response_values[idx+2], self.response_values[idx+3])
            idx+=4
        # GS4 requests processed
        self.GS4_busy = False

    def read_arduino1_complete(self):
        if _debug: PrairieDog._debug("read_arduino1_complete")

        # Publish results to JGCB mqtt topic

        # Arduino requests processed
        self.arduino1_busy = False

    def is_busy(self):

        # check status of each thread
        if self.GS4_busy:
            if _debug: PrairieDog._debug("is busy")
            return True
        elif self.arduino1_busy:
            if _debug: PrairieDog._debug("is busy")
            return True
        else:
            if _debug: PrairieDog._debug("is not busy")
            return False

def mqtt_init(id, topic):
    global mqtt_connected
    if _debug: _log.debug("mqtt_init")

    client = None

    def on_mqtt_connect(client, userdata, flags, rc):
        if rc == 0:
            # success
            if _debug: _log.debug("    - mqtt client %r connected to broker with result code %r", client, rc)
            mqtt_connected = True
            for topic in mqtt_JGCB_topics:
                client.subscribe(topic)
            # syslog
        else:
            if _debug: _log.debug("    - mqtt client failed to connect with result code %r", rc)
            # syslog

    def on_mqtt_disconnect(client, userdata, rc):
        if _debug: _log.debug("    - mqtt client %r disconnected with result code %r", client, rc)
        mqtt_connected = False
        # syslog
        
        reconnect_delay = mqtt_first_reconnect_delay
        while True:
            if _debug: _log.debug("    - try mqtt reconnect in %r seconds", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                if _debug: _log.debug("    - mqtt client reconnected successfully")
                mqtt_connected = True
                # syslog
                return
            except Exception as err:
                if _debug: _log.debug("    - mqtt client reconnect failed with err %r.  Retrying...", err)
                # syslog
                if reconnect_delay == mqtt_max_reconnect_delay:
                    if _debug: _log.debug("    - max mqtt retry delay reached, fallback to LoRa")
                    # TODO: spawn LoRa backup comm here

            reconnect_delay *= mqtt_reconnect_rate
            reconnect_delay = min(reconnect_delay, mqtt_max_reconnect_delay)

    def on_mqtt_message(client, userdata, msg):
        if _debug: _log.debug("mqtt client %r received unsupported message", client)
        # default message client
        # do nothing

    while client == None:
        try:
            client = mqtt.Client()
            if _debug: _log.debug("    - mqtt client created")
            #simple security with username/pw; for better security use SSL/TLS certs
            #client.username_pw_set(username, password)
            client.on_connect = on_mqtt_connect
            client.on_disconnect = on_mqtt_disconnect
            client.on_message = on_mqtt_message

        except Exception as str_error:
            if _debug: _log.debug("    - couldn't create mqtt client")
            time.sleep(10)
            #syslog?

    return client

def on_stuff_msg_callback():
    if _debug: _log.debug("mqtt callback for message received on stuff topic")
    # do stuff if we get a do stuff message



#
#   __main()
#

def main():
    this_application = None
    mqtt_JGCB_client = None

    # parse the command line arguments
    args = ConfigArgumentParser(description=__doc__).parse_args()

    if _debug: _log.debug("initialization")
    if _debug: _log.debug("    - args: %r", args)

    # make an MSTP device object
    mstp_args = {
        '_address': int(args.ini.address),
        '_interface':str(args.ini.interface),
        '_max_masters': int(args.ini.max_masters),
        '_baudrate': int(args.ini.baudrate),
        '_maxinfo': int(args.ini.maxinfo),
    }

    if hasattr(args.ini, 'mstpdbgfile'):
       mstp_args['_mstpdbgfile'] = str(args.ini.mstpdbgfile)

    # create local deice object
    this_device = LocalDeviceObject(ini=args.ini, **mstp_args)
    if _debug: _log.debug("    - this_device: %r", this_device)

    # create mqtt client
    JGCB_mqtt_client = mqtt_init(JGCB_mqtt_ID, mqtt_JGCB_topics)

    # make recurring MSTP BACnet applications
    this_application = PrairieDog(JGCB_interval, JGCB_mqtt_client, this_device, args.ini.address)
    if _debug: _log.debug("    - this_application: %r", this_application)

    #JGCB_mqtt_client.connect(mqtt_broker_address, mqtt_broker_port, mqtt_keep_alive_time)
    #if _debug: _log.debug("    - JGCB mqtt client connect commang issued")
    
    #JGCB_mqtt_client.message_callback_add(mqtt_JGCB_topic, on_stuff_msg_callback)
    #JGCB_mqtt_client.loop_start()
    #if _debug: _log.debug("    - JGCB mqtt client loop started")
    
    _log.debug("running")

    run()
    
    # clean up
    #JGCB_mqtt_client.loop_stop()
    
    _log.debug("fini")


if __name__ == "__main__":
    main()
