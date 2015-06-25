import pyhs2
from timeit import default_timer
import sys


class Hive():

    def __init__(self, select_sql):
        self.query = open(select_sql, 'r')
        self.num_query = 3
    	self.sql = []
    	for i in range(self.num_query):
    		self.sql.append(self.query.readline())

    def run(self, index):

        conn = pyhs2.connect(host='localhost',
                             port=10000,
                             authMechanism="PLAIN",
                             user='',
                             password='',
                             database='benchmark')

        cur = conn.cursor()
    	if index == 3:
            	for i in range(self.num_query):
                		start = default_timer()
                		cur.execute(self.sql[i])
                		for i in cur.fetch():
                		    print i
                		duration = default_timer() - start
                		print "Duration time is %f" % duration
    	else:
    		start = default_timer()
    		cur.execute(self.sql[index])
    		duration = default_timer() - start
    		print "Duration time is %f" % duration
            cur.close()
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2: 
	   sys.exit()
    file_name = sys.argv[1]
    hive = Hive(file_name)
    if len(sys.argv) == 2:
	   hive.run(3)
    else:
	   command = sys.argv[2].lower()
       if command == "scan":
		  hive.run(0)
	   if command == "aggregation":
		  hive.run(1)
	   if command == "join":
		  hive.run(2)
	   if command == "all":
		  hive.run(3)
