#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cx_Oracle
from ConfigParser import ConfigParser
from Queue import Queue


class Init_connection:
    """ """
    def __init__(self, config='connect.conf'):
        """ """
        cp          = ConfigParser()
        cp.read(config)
        self.host   = cp.get('database', 'host')
        self.port   = cp.get('database', 'port')
        self.sid    = cp.get('database', 'sid')
        self.user   = cp.get('database', 'user')
        self.passwd = cp.get('database', 'passwd')
        self.mode   = cp.getboolean('database', 'sysdba')

    #def create_connect_oracle(host='localhost', port='1521', sid='lilo',user='lilo', passwd='lilo', sys=False):
    def create_connect_oracle(self):
        """create and return a connection"""
        dsn    = cx_Oracle.makedsn(host=self.host,
                                port=self.port,
                                sid=self.sid
                                )
        if self.mode:
            con = cx_Oracle.connect(user=self.user,
                                password=self.passwd,
                                dsn=dsn,
                                threaded=True,
                                mode=cx_Oracle.SYSDBA
                                )
        else:
            con = cx_Oracle.connect(user=self.user,
                                password=self.passwd,
                                dsn=dsn,
                                threaded=True
                                )
    
        con.clientinfo = 'cx_Oracle in Python'
        con.module = 'cx_Oracle demo'
        con.action = 'BatchJob #1'

        return con


class Pool:
    """make a connection pool"""
    def __init__(self, callable=None):
        """ """
        self.queue = Queue()
        self.callable = callable
        pass

    #def add_connection(self,*args, **kwargs):
    def add_connection(self):
        """add a connection to the pool"""
        con = self.callable()
        self.queue.put(con)

    def get_connection(self, timeout=10):
        """get a connection in timeout seconds, default 10s"""
        try:
            # Try the special times
            for i in xrange(5):
                con = self.queue.get(timeout=timeout)
                try:
                    con.ping()
                    # We have promise the con is correct while con.ping throw exception.
                    return con
                except cx_Oracle.InterfaceError as e:
                    print e
                    # TODO: We must implemention a connection internal other than outsider.
                    self.add_connection()
        except Queue.Empty:
            print "There exist no other connction, wait"

    def restore_connection(self, con):
        """give back a connection in the pool"""
        self.queue.put(con)

    def monitor():
        """watch the connection queue status"""
        # TODO: watch and re-create a connection
        pass


if __name__ == '__main__':
    in_con = Init_connection(config='connect.conf')
    pool = Pool(callable=in_con.create_connect_oracle)

    #kwargs = {'host':'localhost', 'port':'1521', 'sid':'lilo', 'passwd':'lilo', 'user':'lilo'}
    #args = ()
    #pool.add_connection(host='localhost',port='1521')
    for i in xrange(10):
        pool.add_connection()
    for i in xrange(9):
        con = pool.get_connection()
        cur = con.cursor()
        cur.execute('select sysdate from dual')
        print cur.fetchone()
    print 'The last one'
    for i in xrange(10):
        con = pool.get_connection()
        cur = con.cursor()
        cur.execute('select sysdate from dual')
        print cur.fetchone()
        # Close the cursor other than con.close() because we want use it later
        cur.close()
        # if close it will raise cx_Oracle.InterfaceError
        con.close()
        pool.restore_connection(con)
    #pool.add_connection()
