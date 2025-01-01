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

from GS4Monitor_config import mqtt_broker_address, mqtt_broker_port, mqtt_keep_alive_time
from GS4Monitor_config import mqtt_first_reconnect_delay, mqtt_reconnect_rate, mqtt_max_reconnect_delay
from GS4Monitor_config import JGCB_mqtt_ID

import paho.mqtt.client as mqtt

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

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# polling interval (in seconds)
interval = 5

# point list, set according to your device
point_list = [
#    (99, 'analogValue:1', 'presentValue'),
    (99, 'analogValue:1', 'presentValue'),
    (99, 'analogValue:2', 'presentValue'),
    ]

#
#  Recurring Task to Monitor GS4 Drive System
#  Read each GS4 drive parameter from static point_list using BACnet protocol
#

@bacpypes_debugging
class PrairieDog(MSTPSimpleApplication, RecurringTask):

    def __init__(self, interval, *args):
        if _debug: PrairieDog._debug("__init__ %r %r", interval, args)
        MSTPSimpleApplication.__init__(self, *args)
        # set interval of recurring task (in seconds)
        RecurringTask.__init__(self, interval * 1000)

        # initialize to not busy
        self.GS4_busy = False
        self.arduino1_busy = False
        
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
        self.point_queue = deque(point_list)
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

            # write value to stdout for debugging
            sys.stdout.write(str(value) + '\n')
            if hasattr(value, 'debug_contents'):
                value.debug_contents(file=sys.stdout)
            sys.stdout.flush()

            # save the value
            self.response_values.append(value)

        # no ioError or ioResponse
        else:
            if _debug: PrairieDog._debug._debug("    - GS4 ioError or ioResponse expected")

        # queue the next GS4 request
        deferred(self.next_GS4_request)

    def read_GS4_complete(self):
        if _debug: PrairieDog._debug("read_GS4_complete")

        # for now, dump out the results to screen
        for request, response in zip(point_list, self.response_values):
            print(request, response)

        # Publish results to JGCB mqtt topic

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
    if _debug: _log.debug("mqtt_init")

    def on_mqtt_connect(client, userdata, flags, rc):
        if rc == 0:
            # success
            if _debug: _log._debug("    - mqtt client %r connected to broker with result code %r", client, rc)
            client.subscribe(topic)
            # syslog
        else:
            if _debug: _log._debug("    - mqtt client failed to connect with result code %r", rc)
            # syslog

    def on_mqtt_disconnect(client, userdata, rc)
        if _debug: _log._debug("    - mqtt client %r disconnected with result code %r", client, rc)
        # syslog
        
        reconnect_delay = mqtt_first_reconnect_delay
        while True:
            if _debug: _log._debug("    - try mqtt reconnect in %r seconds", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                if _debug: _log._debug("    - mqtt client reconnected successfully")
                # syslog
                return
            except Exception as err:
                if _debug: _log._debug("    - mqtt client reconnect failed with err %r.  Retrying...", err)
                # syslog
                if reconnect_delay == mqtt_max_reconnect_delay:
                    if _debug: _log._debug("    - max mqtt retry delay reached, fallback to LoRa")
                    # TODO: spawn LoRa backup comm here

            reconnect_delay *= mqtt_reconnect_rate
            reconnect_delay = min(reconnect_delay, mqtt_max_reconnect_delay)

    def on_mqtt_message(client, userdata, msg)
        if _debug: _log._debug("mqtt client %r received unsupported message", client)
        # default message client
        # do nothing

    try:
        client = mqtt.Client(id)
        if _debug: _log._debug("    - mqtt client created")
    except Exception as str_error:
        if _debug: _log._debug("    - couldn't create mqtt client")
        #syslog?

    #simple security; use SSL/TLS certs for better security
    #client.username_pw_set(username, password)
    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message

    return client

def on_stuff_msg_callback()
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

    # set up mqtt client
    JGCB_mqtt_client = mqtt_init(JGCB_mqtt_ID, stuff_topic)
    JGCB_mqtt_client.connect(mqtt_broker_address, mqtt_broker_port, mqtt_keep_alive_time)
    if _debug: _log.debug("    - JGCB mqtt client connect commang issued")
    
    JGCB_mqtt_client.message_callback_add(stuff_topic, on_stuff_msg_callback)
    JGCB_mqtt_client.loop_start()
    if _debug: _log.debug("    - JGCB mqtt client loop started")
    
    # create local deice object
    this_device = LocalDeviceObject(ini=args.ini, **mstp_args)
    if _debug: _log.debug("    - this_device: %r", this_device)

    # make recurring MSTP BACnet applications
    this_application = PrairieDog(interval, this_device, args.ini.address)
    if _debug: _log.debug("    - this_application: %r", this_application)

    _log.debug("running")

    run()
    
    # clean up
    JGCB_mqtt_client.loop_stop()
    
    _log.debug("fini")


if __name__ == "__main__":
    main()
