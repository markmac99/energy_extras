# python script to obtain some extra electricity data from the 
# raw data captured from the Octopus, Enphase and Anker APIs

import argparse
import datetime
import os
import sys
from influxdb import InfluxDBClient
import logging
import requests
from requests.auth import HTTPBasicAuth 
import pandas as pd
import numpy as np
import ssl
from paho.mqtt.publish import multiple


from mqConfig import readConfig

log = logging.getLogger('octopus_misc')


measurementlist = [ 'Anker_E5000_Load_Power', 'Anker_E5000_Battery_Power', 'Anker_E5000_Grid_Power', 'Anker_E5000_PV_Power',
    'Enphase_Envoy_Load_Watts', 'Enphase_Envoy_Battery_Watts', 'Enphase_Envoy_PV_Watts', 'Enphase_Envoy_Grid_Watts']

minilist = ['Anker_E5000_Battery_Power', 'Anker_E5000_PV_Power', 'Enphase_Envoy_Battery_Watts',
                 'Enphase_Envoy_PV_Watts', 'Enphase_Envoy_Grid_Watts']


def getInfluxUrl():
    """
    Retrieve the connection details for connecting to Influx

    Returns
    a tuple of influx URL, user, password, servername, port and databasename
    """

    passwordfile = os.path.expanduser('~/.ssh/influxdb')
    with open(passwordfile) as inf:
        usr = inf.readline().strip()
        pwd = inf.readline().strip()    
    influxserver = 'ohserver'
    influxport = 8086
    influxdatabase = 'openhab'
    influxurl = f'http://{influxserver}:{influxport}/write?db={influxdatabase}'
    return influxurl, usr, pwd, influxserver, influxport, influxdatabase


def addToInflux(tablename, value_flt, value_dt):
    """
    Post a value to an influx table at a specified date

    Arguments
    tablename   [string] table name
    value_flt   [float] value to insert
    value_dt    [datetime] date and time at which to insert it

    Returns
    HTTP return code from requests.get()
    """

    url, usr, pwd, _, _, _ = getInfluxUrl()
    lasttime_ts = int(value_dt.timestamp()*1e9)
    idbdata = f'{tablename},item={tablename} value={value_flt} {lasttime_ts}\n'
    authn = HTTPBasicAuth(usr, pwd)
    ret = requests.post(url, data=idbdata, auth=authn)
    return ret


def getLatestFromInflux(tablename, within='24h'):
    """
    Get latest value from a named table in influx, within the period defined.

    Arguments
    tablename:  [string] the name of a table in influx

    keyword arguments
    within:     [string] the lookback period, eg "30m" to look back 30m. default "24h"

    Returns
    time and value pair {'time':datetime-obj, 'tablename':value}
    """

    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    vals = client.query(f"Select last(value) from {tablename} where time > now() -{within} ")
    if vals:
        data = list(vals.get_points(measurement=tablename))[0]
        if len(data['time'])>20:
            tstamp = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            tstamp = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%SZ')
        return {'time': tstamp, tablename: data['last']}
    else:
        return None


def keepTableUpdated(tablename, val=0):
    """
    Update an influxDB table by copying the latest value to the next half-hour point.
    This is necessary for some tables as the OH persistence module only writes changes to the database
    and so sometimes, we have jumps in the data eg last update was zero at 23:00, next update is hours later to 3000. 

    Arguments
    tablename:  [string] the name of a table in influx

    keyword arguments
    val:        [float] the value to use, if no other value available
    """

    if not getLatestFromInflux(tablename, within='30m'):
        log.info(f'{tablename} not updated within 30m, forcing an update')
        data = getLatestFromInflux(tablename)
        if data:
            val = data[tablename]
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        mins = 30 if now.minute > 30 else 0
        entrytime = datetime.datetime(now.year, now.month, now.day, now.hour, mins, 0, tzinfo=datetime.timezone.utc)
        log.info(f'posting value {val} at time {entrytime.strftime("%Y-%m-%d %H:%M:%S")}')
        ret = addToInflux(tablename, val, entrytime)
        log.info(f'return code {ret.status_code}')
    else:
        log.info('no action required')
    return 


def getResampledData(tablename, startdate, enddate, resample="10min"):
    """
    Resample InfluxDB data at a specified interval. Data are labelled with the right-side 
    of the sample interval eg data from 00:00 to 00:30 is labelled as 00:30. 
    Sampling is exclusive of the start-date and inclusive of the end date eg sampling from 00:00 to 12:00 will
    include times greater than 00:00 and less than or equal to 12:00

    Arguments
    tablename:  [string] the name of a table in influx
    startdate   [datetime] start date (exclusive)
    enddate     [datetime] end date (inclusive)

    keyword arguments
    resample    [string] eg '10min' or '30min' to resample at those intervals

    Returns
    A pandas dataframe containing the data, indexed by time and with a single column 
    named for the tablename

    """

    url, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    ts1 = startdate.strftime('%Y-%m-%dT00:00:00Z')
    ts2 = enddate.strftime('%Y-%m-%dT%H:%M:%SZ')
    log.info(f'getting data for {ts1} to {ts2}')
    vals = client.query(f"Select value from {tablename} where time > '{ts1}' and time <= '{ts2}'")
    if vals:
        df = pd.DataFrame(vals[tablename])
        log.info(f'retrieved {len(df)} rows')
        df['time'] = pd.to_datetime(df['time'], format='mixed')
        df.set_index('time', inplace=True)
        halfhourly = df.resample(resample, label='right').mean()
        #halfhourly.reset_index(inplace=True)
        halfhourly['value'] = halfhourly.value
        halfhourly.dropna(inplace=True)
        halfhourly.rename(columns={'value':tablename}, inplace=True)
        return halfhourly
    else:
        return None


def getAllData(startdate, enddate):
    """
    Get all available data from the Envoy and Anker systems in a single dataframe, 
    resampled at ten minute intervals

    Arguments
    startdate   [datetime] start date (exclusive)
    enddate     [datetime] end date (inclusive)

    Returns
    A pandas dataframe containing the data, indexed by time and with a column per measurement

    """

    vals = []
    for meas in measurementlist:
        vals.append(getResampledData(meas, startdate, enddate, resample='10min'))
    fullset = pd.concat(vals, axis=1)
    fullset.fillna(0, inplace=True)
    return fullset


def getConsumption(startdate, enddate):
    """
    Get true household consumption data. 

    """
    vals = []
    for meas in minilist:
        vals.append(getResampledData(meas, startdate, enddate, resample='10min'))
    fullset = pd.concat(vals, axis=1)
    fullset.fillna(0, inplace=True)
    if hasattr(fullset, 'Anker_E5000_Battery_Power'):
        fullset['total_batt'] = fullset.Anker_E5000_Battery_Power + fullset.Enphase_Envoy_Battery_Watts
        if hasattr(fullset, 'Anker_E5000_PV_Power'):
            fullset['total_pv'] = fullset.Anker_E5000_PV_Power + fullset.Enphase_Envoy_PV_Watts
        else:
            fullset['total_pv'] = fullset.Enphase_Envoy_PV_Watts
        fullset['pos_batt']=np.where(fullset.total_batt <0,0,fullset.total_batt)
        fullset['consumption'] = fullset.pos_batt + fullset.total_pv + fullset.Enphase_Envoy_Grid_Watts
        if hasattr(fullset, 'Anker_E5000_PV_Power'):
            newdf = fullset.drop(columns=['total_batt','total_pv','pos_batt','Anker_E5000_Battery_Power',
                                    'Anker_E5000_PV_Power','Enphase_Envoy_Battery_Watts',
                                    'Enphase_Envoy_PV_Watts','Enphase_Envoy_Grid_Watts'])
        else:
            newdf = fullset.drop(columns=['total_batt','total_pv','pos_batt','Anker_E5000_Battery_Power',
                                    'Enphase_Envoy_Battery_Watts',
                                    'Enphase_Envoy_PV_Watts','Enphase_Envoy_Grid_Watts'])

    else:
        fullset['total_batt'] = fullset.Enphase_Envoy_Battery_Watts
        fullset['total_pv'] = fullset.Enphase_Envoy_PV_Watts
        fullset['pos_batt']=np.where(fullset.total_batt <0,0,fullset.total_batt)
        fullset['consumption'] = fullset.pos_batt + fullset.total_pv + fullset.Enphase_Envoy_Grid_Watts
        newdf = fullset.drop(columns=['total_batt','total_pv','pos_batt','Enphase_Envoy_Battery_Watts',
                                    'Enphase_Envoy_PV_Watts','Enphase_Envoy_Grid_Watts'])

    newdf.reset_index(inplace=True)
    for _,rw in newdf.iterrows():
        tablename = 'HouseElectricityPower'
        dt = rw.time
        val = round(rw.consumption,1)
        addToInflux(tablename, val, dt)

    return newdf


def getLatestConsumption():
    log.info('connecting to MQ')
    broker, mqport, username, passwd = readConfig()
    topic  = 'house/electricity'
    auth = {'username': username, 'password': passwd}
    clientid = 'houseenergy'
    if int(mqport) == 8883:
        tls = {'ca_certs':None, 'cert_reqs':ssl.CERT_REQUIRED, 'tls_version':ssl.PROTOCOL_TLS}
    else:
        tls = None

    log.info('getting data')
    anker_batt = getLatestFromInflux(minilist[0], within='2h')
    anker_pvar = getLatestFromInflux(minilist[1], within='2h')
    envoy_batt = getLatestFromInflux(minilist[2], within='2h')
    envoy_pvar = getLatestFromInflux(minilist[3], within='2h')
    envoy_grid = getLatestFromInflux(minilist[4], within='2h')

    anker_batt = 0 if not anker_batt else anker_batt['Anker_E5000_Battery_Power']
    anker_pvar = 0 if not anker_pvar else anker_pvar['Anker_E5000_PV_Power']
    envoy_batt = 0 if not envoy_batt else envoy_batt['Enphase_Envoy_Battery_Watts']
    envoy_pvar = 0 if not envoy_pvar else envoy_pvar['Enphase_Envoy_PV_Watts']
    envoy_grid = 0 if not envoy_grid else envoy_grid['Enphase_Envoy_Grid_Watts']
        
    consumption = max(0, anker_batt + envoy_batt) +(anker_pvar + envoy_pvar) + envoy_grid
    consumption = round(consumption, 1)
    msgs = []   
    msgs.append((f'{topic}/HouseConsumption', consumption, 1))
    log.info(msgs)
    multiple(msgs=msgs, hostname=broker, port=int(mqport), client_id=clientid, keepalive=60, auth=auth, tls=tls)
    log.info('done')

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description='Perform various actions with energy data in InfluxDB')
    arg_parser.add_argument('action' , metavar='ACTION', type=str, help='action to perform')
    arg_parser.add_argument('item', metavar='ITEM', type=str, help='item to act on')
    cml_args = arg_parser.parse_args()


    logpath = os.path.expanduser('~/logs')
    os.makedirs(logpath, exist_ok=True)
    logname=os.path.join(logpath, 'energy_extras.log')
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(logname, 'a+')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s-%(levelname)s-%(module)s-line:%(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    log.info('Starting')

    action = cml_args.action.lower()
    if action == 'keepupdated':
        if cml_args.item not in measurementlist:
            print('item must be one of ', measurementlist)
            exit(0)
        log.info(f'keeping {cml_args.item} updated')
        keepTableUpdated(cml_args.item)
    elif action == 'updatehousepower':
        getLatestConsumption()