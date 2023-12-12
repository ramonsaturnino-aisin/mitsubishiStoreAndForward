import mariadb
import socket
import os
import logging
import csv  # only used for testing. Remove once read_hosts() is deleted for production.
from time import sleep
from threading import Thread
from os.path import dirname as thisdir, realpath as thispath
from datetime import datetime
from pymelsec import Type4E
from pymelsec.constants import DT
from pymelsec.tag import Tag



# Set app constants and config params
script_root_dir = thisdir(thispath(__file__))
thread_limit = os.cpu_count()
debug = True
this_host = socket.gethostbyname(socket.gethostname())


# Instantiate loggers and log that application has started
def to_event_log(message):
    '''
    Temporary event logger until implementation of logging logger.
    For general script information event logging.
    :param message: (string) Event log message
    '''
    event_log = open(f"{script_root_dir}\\events.log", "a")
    event_log.write(f"{datetime.now()}: {message}")
    event_log.close()


def to_error_log(message):
    '''
    Temporary event logger until implementation of logging logger
    For general exception logging.  Do not use for detailed debugging logs.
    For debugging, use to_debug_log().
    :param message: (string) Event log message
    '''
    error_log = open(f"{script_root_dir}\\errors.log", "a")
    error_log.write(f"{datetime.now()}: {message}")
    error_log.close()


def to_debug_log(message):
    '''
    Temporary event logger until implementation of logging logger.
    For detailed debug information
    :param message: (string) Event log message
    '''
    error_log = open(f"{script_root_dir}\\debug.log", "a")
    error_log.write(f"{datetime.now()}: {message}")
    error_log.close()


def is_numeric(value):
    """
    Accepts any value and determines if it is a string or numeric.
    :param value:
    :return: boolean
    """
    if isinstance(value, float) or isinstance(value, int):
        return True
    else:
        if value.replace(".", "").isnumeric():
            return True
        else:
            return False


# Functions
def set_tag(_tags):
    """
    Builds and returns a tuple of Tag objects.  Case statement needed to control DT object assignments to Tag object.
    :param _tags: A dictionary containing the registry as a key and data type as a value. (i.e., {"D10": "SWORD"})
    :return: A tuple of Melsec Tag objects
    """
    _read_tags = []
    for registry in _tags:
        print(f'data_type: {str(_tags[registry].upper())}')
        match str(_tags[registry].upper()):
            case "BIT":
                _read_tags.append(Tag(device=str(registry), type=DT.BIT)) # Bit = 1 Bit (obviously...)
            case "SWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SWORD)) # Signed Word = 16-bits
            case "UWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.UWORD)) # Unsigned Word = 16-bits
            case "SDWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SDWORD)) # Signed Double Word = 32-bits
            case "UDWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.UDWORD)) # Unsigned Double Word = 32-bits
            case "FLOAT":
                _read_tags.append(Tag(device=str(registry), type=DT.FLOAT)) # Floating Point 32-bits
            case "DOUBLE":
                _read_tags.append(Tag(device=str(registry), type=DT.DOUBLE)) # Double Integer 32-bits
            case "SLWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SLWORD)) # unknown
            case "ULWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.ULWORD)) # unknown
            case _:
                to_error_log(f"Unrecognized data type for registry {registry}")
    return tuple(_read_tags)


def read_host(_plc_host, _plc_port, _plc_type, _poll_rate, _tags):
    '''
    Being developed from read_hosts. Intended for multi-threading. One thread per PLC connection.
    :param _host: (string) IP Address or Hostname of PLC ethernet module
    :param _port: (int) Port number for MELSEC socket on PLC
    :param _plc_type: (string)
    :param _poll_rate: (int) Polling rate in milliseconds
    :param _tags: (dict) key:value pairs of Registry:DataType (i.e., {"D10":"SWORD", "X01:BIT"})
    '''
    dt_now = datetime.now()

    while True:
        read_result = None
        try:
            with Type4E(host=_plc_host, port=_plc_port, plc_type=_plc_type) as plc:
                plc.set_access_opt(comm_type="binary")
                read_result = plc.read(devices=_tags)
        except TimeoutError as te:
            print(f'Communication timeout for PLC host {_plc_host} @ {dt_now}')
            to_error_log(f'Communication timeout for PLC host {_plc_host} @ {dt_now}')

        if read_result != None:
            for tag in read_result:
                if is_numeric(tag.value):
                    # Send number value to DB
                    to_local_sql(
                        plc_host=_plc_host,
                        tag_name="TestTag",
                        register=str(tag.device),
                        num_val=float(tag.value)
                    )
                else:
                    # Send string value to DB
                    to_local_sql(
                        plc_host=_plc_host,
                        tag_name="TestTag",
                        register=str(tag.device),
                        str_val=str(tag.value)
                    )
        else:
            print(f"read_result for host {_plc_host} is empty")

        sleep(_poll_rate)


def to_local_sql(plc_host, tag_name, register, num_val=None, str_val=None):
    """
    Sends tag information to local database.
        Note: This function should only be used to "store" data in the event
                a connection to the remote SQL server is lost.
    :param plc_host: (string)
    :param tag_name: (string)
    :param register: (string)
    :param num_val: (float)
    :param str_val: (string)
    :return: Function does not return a value.
    """
    try:
        conn = mariadb.connect(
            user='svc_storeandforward',
            password='weG3RxkrNIVTjHc1vsOD',
            host=this_host,
            port=3306,
            database='LocalTagStore'
        )
        cur = conn.cursor()
        cur.execute(
            statement="calL p_StoreTagValue(?, ?, ?, ?, ?)",
            data=(plc_host, tag_name, register, num_val, str_val)
        )
        conn.commit()
        print(f"{datetime.now()}: Added host '{plc_host}' data to local database")
    except mariadb.Error as e:
        print(f'{datetime.now()}: {e}')  # Write to service log
        to_error_log(f'{datetime.now()}: {e}')  # Write to application log file
    finally:
        conn.close() # Always close your DB connection


def to_remote_SQL(payload):
    '''
    Send tag data to remote SQL server
    :param payload:
    :return:
    '''
    print(payload)
    # Add SQL connector string
    # Add exception to write to memory cache, if no connection


def read_hosts():

    print(f'Script directory: {script_root_dir}')
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
                to_error_log(f'{dt_now}: Communication timeout for PLC host {_host}')
                if debug:
                    to_debug_log(
                        f'{dt_now}: Communication timeout for PLC {_host}',
                        f'\n\t'
                    )

        sleep(5)


# Main function start
if __name__ == '__main__':
    to_event_log(f"{datetime.now()}: Main() started")

    # static connection settings.  Migrate to config file read in at script init.
    threads = []
    tags = set_tag(
        {
            "D10": "SWORD",
            "SD210": "SWORD",
            "SD211": "SWORD",
            "SD212": "SWORD",
            "SD213": "SWORD",
            "SD214": "SWORD",
            "SD215": "SWORD"
        }
    )
    plc_host_ip_addresses = (
        '192.168.106.40',
        '192.168.106.43'
    )

    # Create and instatiate threading for each host
    for plc_host_ip in plc_host_ip_addresses:
        _kwargs = {
            '_plc_host': plc_host_ip,
            '_plc_port': 5002,
            '_plc_type': 'iQ-R',
            '_poll_rate': 5,
            '_tags': tags
        }
        threads.append(Thread(target=read_host, kwargs=_kwargs))

    for thread in threads: thread.start()
    for thread in threads: thread.join()

# Main function end
