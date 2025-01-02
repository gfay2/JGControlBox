#MQTT Params
# MQTT Client Parameters
mqtt_broker_address = "10.11.33.52"
mqtt_broker_port = 1883
mqtt_JGCB_topics = [
    "cttb/water/pump_station_02/NDATA1/vfd/status/FreqRef",
    "cttb/water/pump_station_02/NDATA1/vfd/status/SetPointPct",
    ]
mqtt_keep_alive_time = 300
# MQTT Reconnect Parameters
mqtt_first_reconnect_delay = 1.25
mqtt_reconnect_rate = 2
mqtt_max_reconnect_delay = 320

# MQTT ID for JG Control Box
JGCB_mqtt_ID = 12


#GS4 Params
# Point List of Parameters to Read from GS4 (4 fields per topic)
# ***NOTE: POINT_LIST MUST MATCH MQTT TOPIC LIST
GS4_point_list = [
    #FreqRef
    (99, 'analogValue:01', 'objectName'),
    (99, 'analogValue:01', 'presentValue'),
    (99, 'analogValue:01', 'units'),
    (99, 'analogValue:01', 'statusFlags'),
    #SetPointPct
    (99, 'analogValue:11', 'objectName'),
    (99, 'analogValue:11', 'presentValue'),
    (99, 'analogValue:11', 'units'),
    (99, 'analogValue:11', 'statusFlags'),
    ]
# JGCB polling interval (sec)
# Current framework polls all sources at the same rate
JGCB_interval = 5