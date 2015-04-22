import pyhs2


class Hive():

    def __init__(self, select_sql):
        self.query = open(select_sql, 'r')
        self.num_query = 3

    def run(self):

        conn = pyhs2.connect(host='localhost',
                             port=10000,
                             authMechanism="PLAIN",
                             user='root',
                             password='test',
                             database='default')

        cur = conn.cursor()
        for index in range(self.num_query):
            cur.execute(self.query.readline())
            for i in cur.fetch():
                print i
        cur.close()
        conn.close()

if __name__ == '__main__':
    hive = Hive("hive.sql")
    hive.run()