#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cx_Oracle
from Queue import Queue


def create_connect_oracle(host='localhost', port='1521', sid='orcl',user='scott', passwd='tiger', sys=False):
    """create and return a connection"""
    dsn = cx_Oracle.makedsn(host=host,
                            port=port,
                            sid=sid
                            )
    if sys:
        con = cx_Oracle.connect(user=user,
                            password=passwd,
                            dsn=dsn,
                            threaded=True,
                            mode=cx_Oracle.SYSDBA
                            )
    else:
        con = cx_Oracle.connect(user=user,
                            password=passwd,
                            dsn=dsn,
                            threaded=True
                            )

    con.clientinfo = 'cx_Oracle in Python'
    con.module = 'cx_Oracle demo'
    con.action = 'BatchJob #1'

    #cur = con.cursor()
    return con


class Pool:
    """make a connection pool"""
    def __init__(self, callable=None):
        """ """
        self.queue = Queue()
        self.callable = callable
        pass

    #def add_connection(self,*args, **kwargs):
    def add_connection(self, args, kwargs):
        """add a connection to the pool"""
        con = self.callable(*args, **kwargs)
        #con = self.callable()
        self.queue.put(con)

    def get_connection(self, timeout=10):
        """get a connection in timeout seconds, default 10s"""
        try:
            con = self.queue.get(timeout=timeout)
            try:
                con.ping()
            except cx_Oracle.InterfaceError as e:
                print e
                # TODO: We must implemention a connection internal other than outsider.
                #self.add_connection()
            # TODO: We must promise the con is correct while con.ping throw exception.
            return con
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
    pool = Pool(callable=create_connect_oracle)
    kwargs = {'host':'localhost', 'port':'1521', 'sid':'lilo', 'passwd':'lilo', 'user':'lilo'}
    args = ()
    #pool.add_connection(host='localhost',port='1521')
    for i in xrange(10):
        pool.add_connection(args, kwargs)
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
