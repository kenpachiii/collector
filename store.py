import logging
import sqlite3

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

class Store:

    def __init__(self, path = './store.db') -> None:

        try: 
            self.con: sqlite3.Connection = sqlite3.connect(path)
            self.cur: sqlite3.Cursor = self.con.cursor()

            self.init()
        except Exception as e:
            logging.error(e)

    def init(self) -> None:
        
        try:
            self.cur.execute('''CREATE TABLE if not exists trades (id TEXT DEFAULT NULL, side TEXT DEFAULT NULL, amount REAL DEFAULT 0, price REAL DEFAULT 0, timestamp INTEGER DEFAULT 0);''')
            self.con.commit()
        except Exception as e:
            logging.error(e)

    def close(self) -> None:
        self.con.commit()
        self.con.close()

    def insert(self, trade: dict) -> bool:

        try:

            columns = ', '.join("`" + str(x) + "`" for x in trade.keys())
            values = ', '.join("'" + str(x) + "'" for x in trade.values())

            insert = 'INSERT INTO %s ( %s ) VALUES ( %s );' % ('trades', columns, values)

            self.cur.execute(insert)
            self.con.commit()
            
        except Exception as e:
            logging.error(e)

    def fetch_trades(self, since = 0):
        try: 
            arg = '''SELECT SUM(amount), AVG(price), ROUND(timestamp / 1800000) * 1800000 FROM trades WHERE timestamp > '{}' GROUP BY ROUND(timestamp / 1800000) * 1800000, side;'''.format(since)
            values = self.cur.execute(arg).fetchall()
            return values
        except Exception as e:
            logging.error(e)