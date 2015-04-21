#!/usr/bin/env python

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class Hive():

    def __init__(self, select_sql):
        self.query = open(select_sql, 'r')

    def run(self):
        try:
            transport = TSocket.TSocket('localhost', 10000)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)

            client = ThriftHive.Client(protocol)
            transport.open()

            client.execute(self.query.readline())
            row = client.fetchOne()
            print "Scan:\n"
            for r in row:
                print r
            print "Finish the first query\n"

            client.execute(self.query.readline())
            row = client.fetchAll()
            print "Join:\n"
            for r in row:
                print r
            print "Finish the second query\n"

            client.execute(self.query.readline())
            row = client.fetchAll()
            print "Aggregation:\n"
            for r in row:
                print r
            print "Finish the third query\n"

            transport.close()

        except Thrift.TException, tx:
            print '%s' % tx.message


if __name__ == '__main__':
    hive = Hive("hive.sql")
    hive.run()
