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
    
    def send(self, name, _type, num, date, core=None):
        if core is not None:
            self.execute("INSERT INTO logging VALUES('{0}','{1}','{2}',{3},{4})".format(_type, name, date,core, num) )
        else:
            self.execute("INSERT INTO logging VALUES('{0}','{1}','{2}','-1',{3})".format(_type, name, date, num) )
