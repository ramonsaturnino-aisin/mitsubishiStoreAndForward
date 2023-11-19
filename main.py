
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


# Set System Constants
script_root_dir = thisdir(thispath(__file__))
thread_limit = os.cpu_count()

# Instantiate loggers and log that application has started

def log_error(message):
    print(message)

# Functions

def set_tags(**_tags):
    '''
    Builds and returns a tuple of Tag objects.  Case statement needed to control DT object assignments to Tag object.
    :param _tags: A dictionary containing the registry as a key and data type as a value. (i.e., {"D10": "SWORD"})
    :return: A tuple of Melsec Tag objects
    '''
    _read_tags = []
    for registry, data_type in _tags.items():
        match data_type:
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
    """
    Establish communication with PLCs and write values to datastore

    Args:
        - _host(string)[Required]: The IP address/hostname of the PLC
        - _port(int)[Required]: The MELSEC port number used on the host
            - options: 5000 - 5009
        - _plc_type(string)[Required]: The plc type of the host
            - options: 'Q', 'L', 'QnA', 'iQ-L', 'iQ-R'
        - _poll_rate(int)[Required]: The polling rate, in milliseconds.
        - _tags(dict)[Required]: A dictionary of tag information
            - format: registry:"registry_type"
            - example: D10:"SWORD" --> Identifies the D10 registry as a signed word
    """

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

        # _hosts = ["192.168.106.40", "192.168.106.43"]
        _port = 5002
        # _port = 5000 # Used to test TimeoutError exception
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
