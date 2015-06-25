from impala.dbapi import connect
from timeit import default_timer
import sys


class Impala():

    def __init__(self, select_sql):
        self.query = open(select_sql, 'r')
		self.num = 3
		self.sql = []
		for i in range(self.num):
			self.sql.append(self.query.readline())
	        conn = connect(host='node1.bm.snaplogic.com', port=21050)
	        self.cur = conn.cursor()
		self.cur.execute("use benchmark")

    def run(self):
		self.scan()
		self.aggregation()
		self.join()

    def scan(self):
		start = default_timer()
        self.cur.execute(self.sql[0])
        #print "Scan:\n"
        #for row in self.cur:
        #    print row
        #print "Finished the first query\n"

		duration = default_timer() - start
		print "Duration time is %f" % duration
	
    def aggregation(self):
		start = default_timer()
        self.cur.execute(self.sql[1])
        #print "Join:\n"
        #for row in self.cur:
        #    print row
        #print "Finished the second query\n"

		duration = default_timer() - start
		print "Duration time is %f" % duration
	
    def join(self):
		start = default_timer()
        self.cur.execute(self.sql[2])
        #print "Aggregation:\n"
        #for row in self.cur:
        #    print row
        #print "Finished the third query\n"

		duration = default_timer() - start
		print "Duration time is %f" % duration

if __name__ == '__main__':
    if len(sys.argv) < 2:
		sys.exit()
    file_name = sys.argv[1]
    impala = Impala(file_name)
    if len(sys.argv) == 2:
		impala.run()
    elif len(sys.argv) > 2:
		command = sys.argv[2]
		if command == "all":
			impala.run()
		if command == "scan":
			impala.scan()
		if command == "aggregation":
			impala.aggregation()
		if command == "join":
			impala.join()
