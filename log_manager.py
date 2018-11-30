import psutil
import time
from db import logger_db
from datetime import datetime
from multiprocessing import Pool

class sys_logger:

    def __init__(self, config):
        self.config = config
        self.nodename = config['main']['Node_name']
        self._parse_conf(config)
        self._attach_loggers()
        self.start()
                
        print(self.list)
    def start(self):
        self.alive = True
        p = Pool(len(self.list))
        p.map(self._spawn_thread, self.list)

    def _spawn_thread(self,arg_list):
        db = logger_db(self.config)
        print(arg_list)
        while(self.alive):
                result = arg_list['logger'](**arg_list['operand_dict'])
                date = datetime.now()
                if type(result) == list:
                    for index, item in enumerate(result):
                        db.send(arg_list['feature_name'],self.nodename, arg_list['feature_type'], item, date, core=index)
                else:    
                    db.send(arg_list['feature_name'],self.nodename, arg_list['feature_type'], result, date, core=None)


    def _attach_loggers(self):
        for index, item in enumerate(self.list):
            if item['feature_type'] == 'cpu_perc':
                self.list[index]['logger'] = psutil.cpu_percent
            elif item['feature_type'] == 'net_up':
                self.list[index]['logger'] = self.net_up
            elif item['feature_type'] == 'net_down':
                self.list[index]['logger'] = self.net_down
            elif item['feature_type'] == 'memory_used':
                self.list[index]['logger'] = self.memory_used
            elif item['feature_type'] == 'disk_read':
                self.list[index]['logger'] = self.disk_read
            elif item['feature_type'] == 'disk_write':
                self.list[index]['logger'] = self.disk_write

    def disk_read(self, perdisk=False,interval=5):
        before = psutil.disk_io_counters(perdisk=perdisk, nowrap=True).read_bytes
        time.sleep(interval)
        after = psutil.disk_io_counters(perdisk=perdisk, nowrap=True).read_bytes
        return after - before
    
    def disk_write(self, perdisk=False,interval=5):
        before = psutil.disk_io_counters(perdisk=perdisk, nowrap=True).write_bytes
        time.sleep(interval)
        after = psutil.disk_io_counters(perdisk=perdisk, nowrap=True).write_bytes
        return after - before

    def memory_used(self, interval):
        time.sleep(interval)
        return psutil.virtual_memory().used

    def net_down(self, pernic=False,interval=5):
        before = psutil.net_io_counters(pernic=pernic, nowrap=True).bytes_recv
        time.sleep(interval)
        after = psutil.net_io_counters(pernic=pernic, nowrap=True).bytes_recv
        return after - before

    def net_up(self, pernic=False,interval=5):
        before = psutil.net_io_counters(pernic=pernic, nowrap=True).bytes_sent
        time.sleep(interval)
        after = psutil.net_io_counters(pernic=pernic, nowrap=True).bytes_sent
        return after - before

    def _parse_conf(self, config):
        self.list = []
        for feature_section_key in config['features']:
            feature_dict = {}
            feature_name = config['features'][feature_section_key]
            feature_dict['feature_name'] = feature_name
            feature_keys = config[feature_name]
            if 'type' in feature_keys:
                feature_type = config[feature_name]['type']
                feature_dict['feature_type'] = feature_type
            else:
                print('ERROR: NO TYPE SPECIFIED IN SECTION',feature_name)
            operand_dict = {}
            if 'interval' in feature_keys:
                feature_interval = int(config[feature_name]['interval'])
                operand_dict['interval'] = feature_interval
            else:
                feature_interval = 20
                feature_dict['feature_interval'] = 20
                print('WARNING: NO INTERVAL SPECIFIED IN SECTION',feature_name, 'TAKING DEFAULT OF 20')
            if 'percpu' in feature_keys:
                operand_dict['percpu'] = bool(config[feature_name]['percpu'])
            if 'pernic' in feature_keys and config[feature_name]['pernic'] == 'True':
                operand_dict['pernic'] = True
            if 'perdisk' in feature_keys and config[feature_name]['perdisk'] == 'True':
                operand_dict['perdisk'] = True
            feature_dict['operand_dict'] = operand_dict
            self.list.append(feature_dict)