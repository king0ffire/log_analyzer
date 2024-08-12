from concurrent.futures import ThreadPoolExecutor,as_completed
import queue
import  mysql.connector
import logging
logger = logging.getLogger(__name__)


class Mypool:
    def __init__(self):
        self.pool=queue.Queue(-1)
        logger.info("start sql")
        config={ "host":"127.0.0.1", "port":3306, "user":"root", "password":"root123", "database":"webapp"}
        with ThreadPoolExecutor() as executor:
            fs=[executor.submit(mysql.connector.connect,**config) for _ in range(1)]
            for f in as_completed(fs):
                self.pool.put(f.result())
        logger.info("connection created")
    def get_connection(self):
        return self.pool.get(block=False)
    def close_connection(self,conn):
        self.pool.put(conn)
    
def initpool(loop=None):
    logger.info("start sql")
    global pool
    pool=Mypool()
    logger.info("connection created")
    
def createtablebypool(fileuid):
    conn=pool.get_connection()
    try:

        conn.cursor().execute(
            "CREATE TABLE dbgitems_%s (id int auto_increment primary key, time VARCHAR(100), errortype VARCHAR(100), \
                    device VARCHAR(255), info VARCHAR(511), event VARCHAR(255), fileid VARCHAR(100), \
                        foreign key (fileid) references fileinfo(fileid))" % fileuid
        )
        conn.commit()
        logger.info("file table created")
    except mysql.connector.errors.ProgrammingError:
        pass
    finally:
        pool.close_connection(conn)
        