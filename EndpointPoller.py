#!/usr/bin/env python3
#!/usr/bin/env python3

import csv
import json
import numpy
import pandas as pd
import requests
import socket
import ssl
import time
import tomllib

from argparse                 import ArgumentParser
from dataclasses              import dataclass, field
from datetime                 import datetime, timedelta
from multiprocessing          import Pool, Value,Manager, Process
from multiprocessing.managers import BaseManager
from pathlib                  import Path
from typing                   import Union,Self


@dataclass(slots=True)
class EndpointPoller:

    endpoint_url  : str
    interval      : int = 60   # interval time in seconds
    duration      : int = 3600 # total time limit in seconds

    _state          : str          = field(init=False)
    _endpoint_ip    : str          = field(init = False)
    _start_time     : datetime     = field(init = False)
    _last_poll_time : datetime     = field(repr=False, init = False)
    _end_time       : datetime     = field(init = False)
    _elapsed_time   : timedelta    = field(repr=False, init = False)
    _data_df        : pd.DataFrame = field(init = False)
    _ssl_data       : dict         = field(init = False)
    _host_data      : dict         = field(init = False)
    

    def __post_init__(self:Self)-> None:
        self._data_df = pd.DataFrame(columns=['timestamp',
                                                              'external_ip',
                                                              'cpu_usage',
                                                              'disk_usage', 
                                                              'memory_usage',
                                                              'database', 
                                                              'redis', 
                                                              'status', 
                                                              'latency'])
        self.get_ssl_info()
        self._state='initialized'

        self._start_time     = None
        self._last_poll_time = None
        self._end_time       = None
        self._elapsed_time   = None


    def poll_endpoint(self:Self) -> dict:
        print(f'polling endpoint {self.endpoint_url} \n')
        self._last_poll_time  = datetime.now()
        poll_time_intial    = self._last_poll_time # assign here for clarity
        response = None

        try:
            response = requests.get(self.endpoint_url)

        except ConnectionError:
            print(str(e))
            pass

        poll_time_final = datetime.now()
        latency = poll_time_final - poll_time_intial 



        if response:
            data = dict(response.json())

            new_data={'timestamp'    : [data['timestamp']],
                      'external_ip'  : [self._endpoint_ip],
                      'cpu_usage'    : [data['metrics']['cpu_usage']],
                      'disk_usage'    : [data['metrics']['disk_usage']],
                      'memory_usage' : [data['metrics']['memory_usage']],
                      'database'     : [data['services']['database']],
                      'redis'        : [data['services']['redis']],
                      'status'       : [data['status']],
                      'latency'      : [latency]
                      }
        else:
            new_data={'timestamp'    : [poll_time_final],
                      'external_ip'  : [self._endpoint_ip],
                      'cpu_usage'    : np.nan,
                      'memory_usage' : np.nan,
                      'database'     : np.nan,
                      'redis'        : np.nan,
                      'status'       : np.nan,
                      'latency'      : np.nan
                      }

        
        self._data_df = pd.concat([self._data_df, pd.DataFrame(new_data)])


    def get_ssl_info(self:Self) -> None:
        url=self.endpoint_url
        if 'https://' in url or 'http://' in url: 
            url = url.replace('https://','')
            url = url.replace('http://','')

        hostname = url.split('/')[0]

        try:
            context = ssl.create_default_context()
            sock    = socket.socket(socket.AF_INET)
            sock.connect((hostname,443))

            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                ip,_ =  ssock.getpeername()
                self._endpoint_ip = ip
                certinfo = ssock.getpeercert()
                expiry_date = certinfo['notAfter']
                host_data = {'hostname':hostname, "endpoint":url, "endpoint_ip":f'{ip}', "ssl_expiry_date":expiry_date}
                self._host_data = host_data
                self._ssl_data = certinfo


        except Exception as e:
            print(str(e))
            return None

        finally:
            ssock.close()
            sock.close()

    

    def run(self:Self)->None:
        current_time = datetime.now()

        if self._state == 'initialized':
            self._start_time=current_time
            self._state='in-progress'
            self.poll_endpoint()

        elapsed_time = current_time - self._start_time

        while elapsed_time <= timedelta(seconds=self.duration):
            current_time = datetime.now()
            elapsed_time = current_time - self._start_time

            if current_time - self._last_poll_time >= timedelta(seconds=self.interval):
                self.poll_endpoint()
            else:
                time.sleep(1)

        self._state == 'done'
        self._end_time = current_time

    def dump(self:Self)->dict:
        # kind of a hack to get around the manager.autoproxy object restrictions
        return {
                "endpoint_url"   : self.endpoint_url,
                "interval"       : self.interval,
                "duration"       : self.duration,
                "state"          : self._state,
                "endpoint_ip"    : self._endpoint_ip,
                "start_time"     : self._start_time,
                "last_poll_time" : self._last_poll_time,
                "endtime"        : self._end_time,
                "elapsed_time"   : self._elapsed_time,
                "data_df"        : self._data_df,
                "ssl_data"       : self._ssl_data,
                "host_data"      : self._host_data
                }

def run_poller(poller) -> None:
    poller.run()

def calculate_uptimes(df) -> pd.DataFrame:
    index = 0
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    cumulative_uptime = timedelta(seconds=0)

    uptime_df = pd.DataFrame(columns=['timestamp','cumulative_uptime'])
    uptime_df['timestamp']=df['timestamp']
    uptime_df = uptime_df.set_index(uptime_df['timestamp'])

    while index < df.shape[0]:

        current_row  = df.iloc[index]
        current_time = current_row['timestamp']
        status       = current_row['status']

        if index == 0:
            cumulative_uptime = timedelta(seconds=0)
            uptime_df.loc[[current_time], 'cumulative_uptime'] =  cumulative_uptime
            index +=1
            continue

        prev_row        = df.iloc[index - 1]
        prev_time       = prev_row['timestamp']
        time_difference = current_time - prev_time
        
        if status == 'success':
            cumulative_uptime += time_difference
            
        elif status != 'success':
            cumulative_uptime = timedelta(seconds=0)

        uptime_df.loc[[current_time], 'cumulative_uptime'] = cumulative_uptime

        index +=1

    uptime_df = uptime_df.drop('timestamp', axis=1)
    return df.merge(uptime_df, on='timestamp')


def calculate_total_uptime(df :pd.DataFrame) -> timedelta:
    fail_count = len(df.loc[ df['status'] != 'success'])
    total_uptime = timedelta(seconds=0)

    if  fail_count == 0:
        total_uptime = df.iloc[df.shape[0]-1]['cumulative_uptime']
        return total_uptime

    else:
        for i in range(df.shape[0]):
            if i == df.shape[0] - 1:
                break

            current_row = df.iloc[i]
            next_row    = df.iloc[i+1]

            if next_row['status'] !='success': 
                total_uptime += current_row['cumulative_uptime']

        return total_uptime


def calculate_latency(df:pd.DataFrame) -> timedelta:
    total_latency = timedelta(seconds=0)
    total_latency = df.loc[df['status'] == 'success', 'latency'].sum()
    successful_count = len(df.loc[df['status'] == 'success'])
    average_uptime_latency = total_latency / successful_count

    return average_uptime_latency


def write_data_out(outfile:Union[str|Path],
                   df: pd.DataFrame,
                   poller_data:dict
                   ) -> None:

    df.set_index('timestamp')
    with open(outfile, 'a+') as fh:
        writer=csv.writer(fh, delimiter=',')

        host_data = poller_data['host_data']
        ssl_data = poller_data['ssl_data']

        #endpoint_url, endpoint_ip, certificate_experation

        initial_data = {
                "internal_ip"            : "redacted",
                "external_ip"            : "redacted",
                "endpoint_url"           : host_data['endpoint'],
                "endpoint_ip"            : host_data['endpoint_ip'],
                "certificate_expiration" : ssl_data['notAfter'],

                }
        writer.writerow(initial_data.keys())
        writer.writerow(initial_data.values())

        writer.writerow([]) # empty rows for clarity

        df.to_csv(fh, index=False)

        writer.writerow([])

        final_data = {'uptime_over_duration':poller_data['uptime_over_duration'],
                      'avg_uptime_latency': poller_data['avg_uptime_latency']}

        writer.writerow(final_data.keys())
        writer.writerow(final_data.values())

        for i in range(3):
            writer.writerow([])


if __name__ == '__main__':
    prog_starttime = datetime.strftime(datetime.now(), '%y%m%d_%H%M%S')
    parser = ArgumentParser()
    parser.add_argument('-t' , '--targets'  , action='store' , dest='targets'  , type=list , default=['https://ac.pfgltd.com/testhealth', 'https://secure.pfgltd.com/testhealth'])
    parser.add_argument('-d' , '--duration' , action='store' , dest='duration' , type=int  , help='total duration in seconds')
    parser.add_argument('-i' , '--interval' , action='store' , dest='interval' , type=int  , help='poll interval time in seconds')
    parser.add_argument('-o' , '--outfile'  , action='store' , dest='outfile'  , type=str  , default=f"./{prog_starttime}-healthcheckdata.csv")
    parser.add_argument('-c' , '--config'   , action='store' , dest='config'   , type=str  , default=None, help='config file to preset command line values. Values specified on commandline take precidence')

    args = parser.parse_args()

    BaseManager.register('EndpointPoller', EndpointPoller)
    manager = BaseManager()
    manager.start()

    poller_proxies = [manager.EndpointPoller(endpoint, 10, 40)
                              for endpoint in args.targets ]

    with Pool(len(poller_proxies)) as p:
        p.map(run_poller,poller_proxies)#, poller_2proxy])
    
    for poller_proxy in poller_proxies:
        poller_data          = poller_proxy.dump()
        data_df              = poller_data['data_df']
        data_df              = calculate_uptimes(data_df)
        avg_uptime_latency   = calculate_latency(data_df)
        uptime_over_duration = calculate_total_uptime(data_df)
        poller_data.update({'uptime_over_duration':uptime_over_duration, 
                            'avg_uptime_latency': avg_uptime_latency})
        write_data_out(args.outfile, data_df, poller_data )
