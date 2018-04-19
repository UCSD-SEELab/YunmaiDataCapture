#!/usr/bin/env python
"""
yunmai_data_processor.py
Michael Ostertag

This script pulls data from a Yunmai smart scale and displays it in the
console. This script is for debug purposes and for initial data gathering for
the IBM for Healthy Aging project.

"""
MQTT_EN = True

import time
import json
import configparser
from bluepy import btle
if (MQTT_EN):
    import paho.mqtt.client as mqtt


class YunmaiDelegate(btle.DefaultDelegate):
    """
    YunmaiDelegate handles all incoming notifications using the protocol 
    outlined by pwnall at https://gist.github.com/pwnall/4ec3cc3d18affa062dd5596f1b4308c9
    
    Yunmai Protocol:
        
        Yunmai smart scale (M1301, M1302, M1303) Bluetooth LE protocol notes

        Commands are written to GATT attribute 0xffe9 of service 0xffe5. Responses come
        as value change notifications for GATT attribute 0xffe4 of service 0xffe0. These
        are 16-bit Bluetooth LE UUIDs, so nnnn is 0000nnnn-0000-1000-8000-00805F9B34FB.
        
        -----
        
        Packet Structure
        
        0d - packet start
        1e - scale software version (only in responses)
        nn - total packet length (includes packet start and CHK)
        nn - command / response opcode
        
        data
        
        CHK - XOR of all bytes except for frame start
        
        Example command: 0d 05 13 00 16
        Example response: 0d 1e 05 05 1e
        
        -------
        
        Response
        
        BE = big endian (most numbers are big endian)
        LE = little endian (the weight stored in a profile is little endian)
        
        00: 0d - packet start
        01: 1e - scale software version
        02: total packet length (includes packet start and CHK)
        03: response type
        
        03: 01 - unfinished weighing
        04-07: date (unix time, seconds) - BE uint32
        08-09: weight - BE uint16 times 0.01
        10: CHK
        
        03: 02 - finished weighing
        04: 00 - historical info
        05-08: date (unix time, seconds) - BE uint32
        09-12: recognized userID - BE uint32
        13-14: weight - BE uint16 times 0.01
        15-16: resistance - BE uint 16
        17-18: fat percentage - BE uint16 times 0.01
        19: CHK
        
        03: 06 - result to user operation
        04: operation type - USER_ADD_OR_UPDATE: 1 | USER_ADD_OR_QUERY: 3 | USER_DELETE: 2
        05-08: userID - 4 bytes, BE
        09: height - in cm
        10: sex - 1 for male
        11: age - in years
        12: waist line - default 85 (0x55)
        13: bust - default 90 (0x5a)
        14-15: basisWeight - default 0, set to previously received weight - LE uint16 times 0.01
        16: display unit - 1 metric
        
        03: 17 - device time
        04-07:
        08: CHK
        
        
        -----
        
        Command
        
        00: 0d - packet start
        01: total packet length (includes packet start and CHK)
        02: command
        
        02: 11 - set time
        03-07: date (unix time, seconds) - BE uint32
        08: fractional second
        09: CHK
        
        02: 17 - read time
        03: CHK
        
        02: 10 - user operation
        03: operation type - USER_ADD_OR_UPDATE: 1 | USER_ADD_OR_QUERY: 3 | USER_DELETE: 2
        04-07: userID - 4 bytes, BE
        08: height - in cm
        09: sex - 1 for male, 2 for female
        10: age - in years
        11: waist line - default 85 (0x55)
        12: bust - default 90 (0x5a)
        13-14: basisWeight - default 0, set to previously received weight
        15: display unit - 1 for metric, 2 for imperial
        16: body type - always 3
        17: CHK
    """
    def __init__(self, scale_name='YunmaiScale', params=None, mqttclient=None):
        btle.DefaultDelegate.__init__(self)
        
        self.message_prev = []
        self.message_now= []
        self.list_parsed_msg = []
        self.client = mqttclient
        self.scale_name = scale_name

    def handleNotification(self, cHandle, data):
        # Check that cHandle is from the correct characteristic (0xffe4) and 
        # service (0xffe0)
        print('{0}: Packet Received'.format(self.scale_name))
        
        # Process incoming message
        """
        00: 0d - packet start
        01: 1e - scale software version
        02: total packet length (includes packet start and CHK)
        03: response type
        """
        
        data = [ord(element) for element in data]
        print([hex(element) for element in data])
        
        # Check packet start (bit: 0x00, value: 0x0d)
        if (data[0] != 0x0d):
            print('{0}: ERROR. Packet start is incorrect'.format(self.scale_name))
            return
        
        if (len(data) < 4):
            print('{0}: ERROR. Packet incomplete'.format(self.scale_name))
            return
        
        self.message_prev = self.message_now[:]
        self.message_now = data
        
        message_type = data[3]
        
        if (message_type == 0x01):
            """
            Data packet format for unfinished weighing
            03: 01 - unfinished weighing
            04-07: date (unix time, seconds) - BE uint32
            08-09: weight - BE uint16 times 0.01
            10: CHK
            """
            datetime = data[4] << (8*3)
            datetime += data[5] << (8*2)
            datetime += data[6] << (8*1)
            datetime += data[7]

            weight = data[8] << (8*1)
            weight += data[9]
            weight *= 0.01
            
            print('{0}: {1:d}  {2:3.2f} kg...'.format(self.scale_name, datetime, weight))            
            
            return
        
        elif (message_type == 0x02):
            """
            Data packet format for finished weighing
            03: 02 - finished weighing
            04: 00 - historical info
            05-08: date (unix time, seconds) - BE uint32
            09-12: recognized userID - BE uint32
            13-14: weight - BE uint16 times 0.01
            15-16: resistance - BE uint 16
            17-18: fat percentage - BE uint16 times 0.01
            19: CHK
            """
            datetime = data[5] << (8*3)
            datetime += data[6] << (8*2)
            datetime += data[7] << (8*1)
            datetime += data[8]
            
            userid = data[9] << (8*3)
            userid += data[10] << (8*2)
            userid += data[11] << (8*1)
            userid += data[12]
            
            weight = data[13] << (8*1)
            weight += data[14]
            weight *= 0.01
            
            resistance = data[15] << (8*1)
            resistance += data[16]
            
            fat = data[17] << (8*1)
            fat += data[18]
            fat *= 0.01
            
            dict_parsed_msg = {
                    'datetime' : datetime,
                    'userid' : userid,
                    'weight' : weight,
                    'resistance' : resistance,
                    'fat' : fat
                    }
            print('{0}: {1}  {2:3.2f} kg, {3:3.1f} % fat, {4:3.0f} ohm'.format(self.scale_name, datetime, weight, fat, resistance)) 
            self.list_parsed_msg.append(dict_parsed_msg)
            if (self.client):
                self.client.publish('YunmaiScaleE35F/raw', json.dumps(dict_parsed_msg))
            
        elif (message_type == 0x17):
            # data packet from time check
            return
        else:
            print('{0}: message type {1} was unprocessed'.format(self.scale_name ,message_type))
        
        return         
         
def process_message(client, userdata, message): #add callback function
    data = message.payload
    print('MQTT: message received')
    print('MQTT: {0} {1}'.format(message.topic, str(message.payload)))
         
          
if __name__ == '__main__':
    # Load configuration parameters
    config = configparser.ConfigParser()
    config.read('config_yunmai.cfg')
    
    try:
        scale_name = config.get('Device', 'name') 
        scale_address = config.get('Device', 'address')
    except:
        print('Failed to parse scale configuration script')
        return
    
    # Initialize MQTT Connection
    if (MQTT_EN):
        config.read('config_mqtt.cfg')
        try:
            broker_address = config.get('Broker', 'address')
            broker_port = config.getint('Broker', 'port')
            broker_secure = config.getboolean('Broker', 'secure')
            topic_ctl = config.get('Topics', 'subscribe')
            topic_data = config.get('Topics', 'publish')
            
            client = mqtt.Client(client_id=scale_name, 
                                 clean_session=True,
                                 protocol=mqtt.MQTTv31) 
            
            if (broker_secure):
                broker_uid = config.get('Broker', 'username')
                broker_pw = config.get('Broker', 'password')
                client.username_pw_set('admin', 'IBMProject$')
            
            client.on_message = process_message
            print('MQTT: Connecting to broker {0}:{1}'.format(broker_address, broker_port))
            client.connect(host=broker_address, port=broker_port)
            client.loop_start()

            print('MQTT: Subscribing to topic {0}'.format(topic_ctl))
            client.subscribe(topic_ctl) 
    else:
        client = None
    
    dev_scale = btle.Peripheral() # Initializes
    dev_scale.setDelegate( YunmaiDelegate(mqttclient=client, scale_name=scale_name) )
    scale_connected = False
    scale_chr_name_h = 0x2a00
    scale_chr_name = None

    time_run = 4*24*3600 # 4 days in seconds    
    time_start = time.time()
    
    # Main loop
    
    while (abs(time.time() - time_start) < time_run):
        if (scale_connected):
            try:
                if dev_scale.waitForNotifications(2.0):
                    # handleNotification() was called
                    continue
                else:
                    """
                    # Test for checking services and characteristics
                    list_services = dev_scale.getServices()
                    print('List of services:')
                    for ind, service in enumerate(list_services):
                        print('  {0}: {1}'.format(ind, service.uuid))
                        
                    
                    list_characteristics = dev_scale.getCharacteristics()
                    print('List of characteristics:')
                    for ind, characteristic in enumerate(list_characteristics):
                        print('  {0}: {1} {2}'.format(ind, characteristic.uuid,
                              characteristic.supportsRead()))

                    print('Test characteristic access:')
                    scale_chr_name = dev_scale.getCharacteristics(uuid=scale_chr_name_h)
                    for ind, characteristic in enumerate(scale_chr_name):
                        print('  {0}: {1} {2}'.format(ind, characteristic.uuid,
                              characteristic.supportsRead()))
                        if (characteristic.supportsRead()):
                            temp = characteristic.read()
                            print(temp)
                        else:
                            print('  Cannot read')
                    """
                    
                    # print('{0}: Waiting for packet...'.format(scale_address))
                    
            except btle.BTLEException as ex:
                if (ex.code == btle.BTLEException.DISCONNECTED):
                    scale_connected = False
                    print('{0}: Device disconnected.'.format(scale_address))
        else:
            try:
                print('{0}: Establishing connection.'.format(scale_address))
                dev_scale.connect(scale_address)
                # scale_chr_name_h = getCharacteristics
                print('{0}: Connected.'.format(scale_address))
                scale_connected = True
                
            except btle.BTLEException as ex:
                print('{0}: Connection failed ({1}).'.format(scale_address, ex))

    if (MQTT_EN):
        client.loop_stop()
