from impala.dbapi import connect


class Impala():

    def __init__(self, select_sql):
        self.query = open(select_sql, 'r')
        conn = connect(host='localhost', port=21050)
        self.cur = conn.cursor()

    def run(self):
        self.cur.execute(self.query.readline())
        print "Scan:\n"
        for row in self.cur:
            print row
        print "Finished the first query\n"

        self.cur.execute(self.query.readline())
        print "Join:\n"
        for row in self.cur:
            print row
        print "Finished the second query\n"

        self.cur.execute(self.query.readline())
        print "Aggregation:\n"
        for row in self.cur:
            print row
        print "Finished the third query\n"


if __name__ == '__main__':
    impala = Impala("impala.sql")
    impala.run()