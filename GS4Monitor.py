#!/usr/bin/env python3

"""
Mutliple Read Property

This application has a static list of points that it would like to read.  It reads the
values of each of them in turn and then quits.
"""

import sys
#import time

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

# globals
this_application = None

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
#

@bacpypes_debugging
class GS4PrairieDog(MSTPSimpleApplication, RecurringTask):

    def __init__(self, interval, *args):
        if _debug: GS4PrairieDog._debug("__init__ %r %r", interval, args)
        MSTPSimpleApplication.__init__(self, *args)
        # set interval of recurring task (in seconds)
        RecurringTask.__init__(self, interval * 1000)

        # busy for initialization
        self.GS4_busy = True
        self.arduino_busy = False

        # initiate whois for GS4 device
        # TODO

        # initialize to not busy
        self.GS4_busy = False

        # install the task
        self.install_task()
        if _debug: GS4PrairieDog._debug("    - install_task")

    def process_task(self):
        if _debug: GS4PrairieDog._debug("process_task")

        # check to see if task is idle
        if self.is_busy():
            return

        # now we are busy
        self.GS4_busy = True
        #self.arduino_busy = True

        # turn the point list into a queue
        self.point_queue = deque(point_list)
        if _debug: GS4PrairieDog._debug("    - deque point list %r", self.point_queue)

        # clear out response value list
        self.response_values = []

        # Non-blocking?
        # read next GS4 data
        self.next_GS4_request()

        # read next Arduino sensor outputs
        # TODO self.next_arduino_request()

        # send MQTT message
        # TODO

    def next_GS4_request(self):
        if _debug: GS4PrairieDog._debug("next_GS4_request")

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
        if _debug: GS4PrairieDog._debug("    - request: %r", request)

        # make an IOCB
        iocb = IOCB(request)
        if _debug: GS4PrairieDog._debug("    - iocb: %r", iocb)

        # set a callback for the request
        iocb.add_callback(self.GS4_request_callback)
        if _debug: GS4PrairieDog._debug("    - GS4 request callback set")

        # give the iocb to the application
        self.request_io(iocb)

    def GS4_request_callback(self, iocb):
        if _debug: GS4PrairieDog._debug("GS4_request_callback %r", iocb)

        # do something for error/reject/abort
        if iocb.ioError:
            sys.stdout.write(str(iocb.ioError) + '\n')
            if _debug: GS4PrairieDog._debug("    - error: %r", iocb.ioError)
            self.response_values.append(iocb.ioError)

        # iocb successful
        elif iocb.ioResponse:
            apdu = iocb.ioResponse

            # should be an ack
            if not isinstance(apdu, ReadPropertyACK):
                if _debug: GS4PrairieDog._debug("    - not an ack")
                stop()

            # find the datatype
            datatype = get_datatype(apdu.objectIdentifier[0], apdu.propertyIdentifier)
            if _debug: GS4PrairieDog._debug("    - datatype: %r", datatype)
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
            if _debug: GS4PrairieDog._debug("    - value: %r", value)

            # write value to stdout for debugging
            sys.stdout.write(str(value) + '\n')
            if hasattr(value, 'debug_contents'):
                value.debug_contents(file=sys.stdout)
            sys.stdout.flush()

            # save the value
            self.response_values.append(value)

        # no ioError or ioResponse
        else:
            if _debug: GS4PrairieDog._debug._debug("    - GS4 ioError or ioResponse expected")

        # queue the next GS4 request
        deferred(self.next_GS4_request)

    def read_GS4_complete(self):
        if _debug: GS4PrairieDog._debug("read_GS4_complete")

        # for now, dump out the results to screen
        for request, response in zip(point_list, self.response_values):
            print(request, response)

        # no busy GS4 requests
        self.GS4_busy = False

    def read_arduino_complete(self):
        if _debug: GS4PrairieDog._debug("read_arduino_complete")

        self.arduino_busy = False

    def is_busy(self):

        # check status of each thread
        if self.GS4_busy:
            if _debug: GS4PrairieDog._debug("is busy")
            return True
        elif self.arduino_busy:
            if _debug: GS4PrairieDog._debug("is busy")
            return True
        else:
            if _debug: GS4PrairieDog._debug("is not busy")
            return False


#
#   __main()
#

def main():
    global this_application

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

    this_device = LocalDeviceObject(ini=args.ini, **mstp_args)
    if _debug: _log.debug("    - this_device: %r", this_device)

    # make a recurring application
    this_application = GS4PrairieDog(interval, this_device, args.ini.address)
    if _debug: _log.debug("    - this_application: %r", this_application)

    _log.debug("running")

    run()

    _log.debug("fini")


if __name__ == "__main__":
    main()
