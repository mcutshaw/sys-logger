#!/usr/bin/python3
import hashlib
import configparser
import pymysql

class logger_db:

    def __init__(self,config):
        try:
            self.host = config['Database']['Host']
            self.user = config['Database']['User']
            self.database = config['Database']['DB']
            self.password = config['Database']['Password']

        except Exception as e:
            print("Config Error!")
            print(e)
            exit()
        try:    
            self.connect()
        except:
            print("Database Error!")
        tables = self.execute("show tables;")         
        if(('logging',) not in tables): 

            self.execute('''CREATE TABLE logging
                            (type VARCHAR(100) NOT NULL,
                            node_name VARCHAR(100) NOT NULL,
                            feature_name VARCHAR(100) NOT NULL,
                            date DATETIME NOT NULL,
                            core INT,
                            value DECIMAL(32,2) NOT NULL);''')

    def close(self):
        self.conn.close()

    def connect(self):
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.database)
        self.cur = self.conn.cursor()

    def execute(self,command):
        self.connect()
        self.cur.execute(command)
        self.conn.commit()
        text_return = self.cur.fetchall()
        self.close()
        return text_return

    def executevar(self,command,operands):
        self.connect()
        self.cur.execute(command,operands)
        print('chec')

        self.conn.commit()
        text_return = self.cur.fetchall()
        self.close()
        return text_return
    
    def send(self, node_name, feature_name, _type, num, date, core=None):
        if core is not None:
            #print("INSERT INTO logging VALUES('{0}','{1}','{2}','{3}',{4},{5})".format(_type, feature_name, node_name, date,core, num))
            self.execute("INSERT INTO logging VALUES('{0}','{1}','{2}','{3}',{4},{5})".format(_type, feature_name, node_name, date,core, num) )
        else:
            #print("INSERT INTO logging VALUES('{0}','{1}','{2}','{3}','-1',{4})".format(_type, feature_name, node_name, date, num))
            self.execute("INSERT INTO logging VALUES('{0}','{1}','{2}','{3}','-1',{4})".format(_type, feature_name, node_name, date, num) )

    def get_data(self, _type):
        print("SELECT value FROM logging WHERE type='{0}'".format(_type))
        results = self.execute("SELECT value,date FROM logging WHERE type='{0}'".format(_type) )
        nums = [item[0] for item in results]
        dates = [item[1] for item in results]
        return (nums,dates)