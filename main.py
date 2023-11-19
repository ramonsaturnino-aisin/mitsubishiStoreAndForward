
import os
import logging
from time import sleep
import csv
import threading
from os.path import dirname, realpath
from datetime import datetime
from pymelsec import Type4E
from pymelsec.constants import DT
from pymelsec.tag import Tag


# Set app constants and config params
script_root_dir = thisdir(thispath(__file__))
thread_limit = os.cpu_count()


# Instantiate loggers and log that application has started
def to_event_log(message):
    '''
    Temporary event logger until implementation of logging logger
    :param message: (string) Event log message
    '''
    event_log = open(f"{cur_wrk_dir}\\events.log", "a")
    event_log.write(f"{datetime.now()}: {message}")
    event_log.close()

def to_error_log(message):
    '''
    Temporary event logger until implementation of logging logger
    :param message: (string) Event log message
    '''
    error_log = open(f"{cur_wrk_dir}\\errors.log", "a")
    error_log.write(f"{datetime.now()}: {message}")
    error_log.close()


# Functions
def set_tags(**_tags):
    '''
    Builds and returns a tuple of Tag objects.  Case statement needed to control DT object assignments to Tag object.
    :param _tags: A dictionary containing the registry as a key and data type as a value. (i.e., {"D10": "SWORD"})
    :return: A tuple of Melsec Tag objects
    '''
    _read_tags = []
    for registry, data_type in _tags.items():
        match str(data_type).upper():
            case "BIT":
                _read_tags.append(Tag(device=str(key), type=DT.BIT)) # Bit = 1 Bit (obviously...)
            case "SWORD":
                _read_tags.append(Tag(device=str(key), type=DT.SWORD)) # Signed Word = 16-bits
            case "UWORD":
                _read_tags.append(Tag(device=str(key), type=DT.UWORD)) # Unsigned Word = 16-bits
            case "SDWORD":
                _read_tags.append(Tag(device=str(key), type=DT.SDWORD)) # Signed Double Word = 32-bits
            case "UDWORD":
                _read_tags.append(Tag(device=str(key), type=DT.UDWORD)) # Unsigned Double Word = 32-bits
            case "FLOAT":
                _read_tags.append(Tag(device=str(key), type=DT.FLOAT)) # Floating Point 32-bits
            case "DOUBLE":
                _read_tags.append(Tag(device=str(key), type=DT.DOUBLE)) # Double Integer 32-bits
            case "SLWORD":
                _read_tags.append(Tag(device=str(key), type=DT.SLWORD)) # unknown
            case "ULWORD":
                _read_tags.append(Tag(device=str(key), type=DT.ULWORD)) # unknown
            case _:
                raise UserWarning(f"Unrecognized data type for registry {registry}")
        return tuple(_read_tags)


def read_host(_host, _port, _plc_type, _poll_rate, **_tags):
    '''
    Being developed from read_hosts. Intended for multi-threading. One thread per PLC connection.
    :param _host: (string) IP Address or Hostname of PLC ethernet module
    :param _port: (int) Port number for MELSEC socket on PLC
    :param _plc_type: (string)
    :param _poll_rate: (int) Polling rate in milliseconds
    :param _tags: (dict) key:value pairs of Registry:DataType (i.e., {"D10":"SWORD", "X01:BIT"})
    :return:
    '''
    _read_tags = []
    for key, value in _tags.items():
        _read_tags.append(Tag(device=str(key), type=DT.str(value)))

    dt_now = datetime.now()
    try:
        with Type4E(host=_host, port=_port, plc_type=_plc_type) as plc:
            plc.set_access_opt(comm_type="binary")
            read_result = plc.read(devices=_read_tags)
            print(f'host:{_host}')
            for tag in read_result:
                print(
                    f'\tdevice:{tag.device}',
                    f'value:{tag.value}',
                    f'type:{tag.type}',
                    f'error:{tag.error}',
                    f'datetime:{dt_now}'
                )
                with open(f'{cur_wrk_dir}\\results.csv', 'a', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow([_host, tag.device, tag.value, tag.type, tag.error, str(dt_now)])
    except TimeoutError as te:
        print(f'Communication timeout for PLC host {_host} @ {dt_now}')

    sleep(5)


def read_hosts():

    print(f'Current working directory: {cur_wrk_dir}')
    while True:
        _read_tags = [
            Tag(device="D10", type=DT.SWORD),
            Tag(device="SD210", type=DT.SWORD),
            Tag(device="SD211", type=DT.SWORD),
            Tag(device="SD212", type=DT.SWORD),
            Tag(device="SD213", type=DT.SWORD),
            Tag(device="SD214", type=DT.SWORD),
            Tag(device="SD215", type=DT.SWORD)
        ]

        _hosts = ["192.168.106.40", "192.168.106.43"]
        _port = 5002
        _plc_type = "iQ-R"

        for _host in _hosts:
            dt_now = datetime.now()
            try:
                with Type4E(host=_host, port=_port, plc_type=_plc_type) as plc:
                    plc.set_access_opt(comm_type="binary")
                    read_result = plc.read(devices=_read_tags)
                    print(f'host:{_host}')
                    for tag in read_result:
                        print(
                            f'\tdevice:{tag.device}',
                            f'value:{tag.value}',
                            f'type:{tag.type}',
                            f'error:{tag.error}',
                            f'datetime:{dt_now}'
                        )
                        with open(f'{script_root}\\results.csv', 'a', newline='') as csvfile:
                            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                            csvwriter.writerow([_host, tag.device, tag.value, tag.type, tag.error, str(dt_now)])
            except TimeoutError as te:
                print(f'Communication timeout for PLC host {_host} @ {dt_now}')


        sleep(5)


# Main function start
if __name__ == '__main__':
    #read_tags()
    tags= {
        "D10":"SWORD"
    }
    set_tag(_tags=tags)

# Main function end
