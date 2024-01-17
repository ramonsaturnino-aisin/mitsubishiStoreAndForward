import mariadb
import socket
import os
from matplotlib import pylab
from pylab import *
import logging
import memory_profiler
from memory_profiler import profile
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
from time import sleep
from threading import Thread
from os.path import dirname as thisdir, realpath as thispath
from datetime import datetime
import pypyodbc as odbc
from pymelsec import Type4E
from pymelsec.constants import DT
from pymelsec.tag import Tag

# Set app constants and config params
script_root_dir = thisdir(thispath(__file__))
thread_limit = os.cpu_count()
debug = True
this_host = socket.gethostbyname(socket.gethostname())


fp=open("report_to_event_log.log", "w+")
@profile(stream=fp)
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



@profile(stream=fp)
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

@profile(stream=fp)
def to_debug_log(message):
    '''
    Temporary event logger until implementation of logging logger.
    For detailed debug information
    :param message: (string) Event log message
    '''
    error_log = open(f"{script_root_dir}\\debug.log", "a")
    error_log.write(f"{datetime.now()}: {message}")
    error_log.close()

@profile(stream=fp)
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

@profile(stream=fp)
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
                _read_tags.append(Tag(device=str(registry), type=DT.BIT))  # Bit = 1 Bit (obviously...)
            case "SWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SWORD))  # Signed Word = 16-bits
            case "UWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.UWORD))  # Unsigned Word = 16-bits
            case "SDWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SDWORD))  # Signed Double Word = 32-bits
            case "UDWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.UDWORD))  # Unsigned Double Word = 32-bits
            case "FLOAT":
                _read_tags.append(Tag(device=str(registry), type=DT.FLOAT))  # Floating Point 32-bits
            case "DOUBLE":
                _read_tags.append(Tag(device=str(registry), type=DT.DOUBLE))  # Double Integer 32-bits
            case "SLWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.SLWORD))  # unknown
            case "ULWORD":
                _read_tags.append(Tag(device=str(registry), type=DT.ULWORD))  # unknown
            case _:
                to_error_log(f"Unrecognized data type for registry {registry}")
    return tuple(_read_tags)

@profile(stream=fp)
def send_email():
    msg = EmailMessage()
    msg['Subject'] = 'This is a test'
    msg.set_content('This is a test (in case both databases were not able to reach a connection) :)')
    msg['From'] = 'storeandforward@aisinnc.com'
    msg['To'] = ['ramonsaturnino@aisinnc.com']

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('192.168.37.5')
    print("email sent successfully")
    s.send_message(msg)
    sleep(30)
    s.quit()

@profile(stream=fp)
def connection_verification():
    try:
        conn = odbc.connect(f"""
                DRIVER={{{'SQL SERVER'}}};
                SERVER={'S-MES-DB-DEV'};
                DATABASE={'LocalTagStore'};
                Trust_Connection=yes;
                uid=svc_storeandforward;
                pwd=weG3RxkrNIVTjHc1vsOD;
                """)
        conn.close()
        print(f"\n{conn}")
        return True
    except Exception as e:
        print(f"\nAn error occurred with remote db: {e}")
        return False


@profile(stream=fp)
def local_conn_verification():
    try:
        conn = mariadb.connect(
            user='svc_storeandforward',
            password='weG3RxkrNIVTjHc1vsOD',
            host='192.168.106.44',
            port=3306,
            database='LocalTagStore'
        )
        conn.cursor()
        conn.close()
        print(f"\n{conn}")
        return True
    except mariadb.Error as e:
        print(f"\nAn error occurred with local db: {e}\n")
        return False


@profile(stream=fp)
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
                    if connection_verification():
                        to_remote_sql(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            num_val=float(tag.value)
                        )
                    elif local_conn_verification():
                        to_local_sql(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            num_val=float(tag.value)
                        )
                    else:
                        send_email()
                        to_csv(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            num_val=float(tag.value)
                        )
                else:  # is_string(tag.value)
                    if connection_verification():
                        to_remote_sql(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            str_val=str(tag.value)
                        )
                    elif local_conn_verification():
                        to_local_sql(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            str_val=str(tag.value)
                        )
                    else:
                        send_email()
                        to_csv(
                            plc_host=_plc_host,
                            tag_name="TestTag",
                            register=str(tag.device),
                            str_val=str(tag.value)
                        )
        else:
            print(f"read_result for host {_plc_host} is empty")
            print("~~~~~~~~~~~~~~~~~~~~~")

        sleep(_poll_rate)


@profile(stream=fp)
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
        conn.close()  # Always close your DB connection


@profile(stream=fp)
def to_remote_sql(plc_host, tag_name, register, num_val=None, str_val=None):
    """
        Sends tag information to REMOTE database.
            Note: This function should be used to store data as a Main DB.
        :param plc_host: (string)
        :param tag_name: (string)
        :param register: (string)
        :param num_val: (float)
        :param str_val: (string)
        :return: Function does not return a value.
        """
    try:
        conn = odbc.connect(
            f"""
                DRIVER={{{'SQL SERVER'}}};
                SERVER={'S-MES-DB-DEV'};
                DATABASE={'LocalTagStore'};
                Trust_Connection=yes;
                uid=svc_storeandforward;
                pwd=weG3RxkrNIVTjHc1vsOD;
                """
        )
        cursor = conn.cursor()
        params = [plc_host, tag_name, register, num_val, str_val]  # coming from other function
        cursor = conn.cursor()
        (cursor.execute("{CALL p_StoreTagValue(?, ?, ?, ?, ?)}", params))
        # Commit the changes
        conn.commit()
        print(f"{datetime.now()}: Added host '{plc_host}' data to remote database")
    except Exception as e:
        print(f"An error occurred: {e}")
        to_error_log(f'{datetime.now()}: {e}')  # Write to application log file
    finally:
        # Close the cursor and connection when done
        cursor.close()
        conn.close()


@profile(stream=fp)
def to_csv(plc_host, tag_name, register, num_val=None, str_val=None):
    with open(f'{script_root_dir}\\results.csv', 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow([plc_host, tag_name, register, num_val, str_val])
    print("Sent to csv file")

    
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
