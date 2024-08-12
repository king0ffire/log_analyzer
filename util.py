import queue
import threading


def mapget(map, key):
    if key in map:
        return map[key]
    else:
        return 0
    
class DBWriter:
    def __init__(self, conn,precompiledsql,fileuid, batch_size=1000):
        self.batch_size = batch_size
        self.sql=precompiledsql
        self.targettable=precompiledsql.split(' ')[2]
        self.targetfileuid=fileuid
        self.conn=conn
        self.queue = queue.Queue(-1)
        self.thread = threading.Thread(target=self._db_worker_2, daemon=True)
        self.thread.start()

    def _db_worker_2(self):
        batch = []
        cursor=self.conn.cursor()
        while True:
            data = self.queue.get()
            if data is None:# 接收到结束信号，退出线程
                if batch:
                    cursor.executemany(self.sql,batch)
                    return
            batch.extend(data)
            try:
                while True:
                    data = self.queue.get(block=False)
                    if data is None:# 接收到结束信号，退出线程
                        if batch:
                            cursor.executemany(self.sql,batch)
                            return
                    batch.extend(data)
            except queue.Empty:
                cursor.executemany(self.sql,batch)
                batch.clear()
            
            
    def _db_worker(self):
        batch = []
        cursor=self.conn.cursor()
        while True:
            data = self.queue.get()
            if data is None:
                break  # 接收到结束信号，退出线程
            batch.extend(data)
            if len(batch) >= self.batch_size:  # 当积累到足够的数据量时，进行一次批量写入
                
                cursor.executemany(self.sql,batch)
                batch.clear()
        # 处理剩余的数据
        if batch:
            cursor.executemany(self.sql,batch)

    def _db_worker_3(self):
        batch = []
        cursor=self.conn.cursor()
        while True:
            data = self.queue.get()
            if data is None:
                break  # 接收到结束信号，退出线程
            batch.extend(data)
            if len(batch) >= self.batch_size:  # 当积累到足够的数据量时，进行一次批量写入
                sql="insert into "+self.targettable+" (time, errortype, device,info,event) values "
                sqlmain=",".join(str(item) for item in batch)
                cursor.execute(sql+sqlmain)
                batch.clear()
        # 处理剩余的数据
        if batch:
            sql="insert into "+self.targettable+" (time, errortype, device,info,event) values "
            sqlmain=",".join(str(item) for item in batch)
            cursor.execute(sql+sqlmain)
            
    def _db_worker_4(self):
        batch = []
        cursor=self.conn.cursor()
        while True:
            data = self.queue.get()
            if data is None:# 接收到结束信号，退出线程
                if batch:
                    sql="insert into "+self.targettable+" (time, errortype, device,info,event) values "
                    sqlmain=",".join(str(item) for item in batch)
                    cursor.execute(sql+sqlmain)
                    return
            batch.extend(data)
            try:
                while True:
                    data = self.queue.get(block=False)
                    if data is None:# 接收到结束信号，退出线程
                        if batch:
                            sql="insert into "+self.targettable+" (time, errortype, device,info,event) values "
                            sqlmain=",".join(str(item) for item in batch)
                            cursor.execute(sql+sqlmain)
                            return
                    batch.extend(data)
            except queue.Empty:
                sql="insert into "+self.targettable+" (time, errortype, device,info,event) values "
                sqlmain=",".join(str(item) for item in batch)
                cursor.execute(sql+sqlmain)
                batch.clear()
    def add_data(self, data):
        """向队列中添加数据"""
        self.queue.put(data)

    def add_batch(self,batch):
        self.queue.put(batch)
    def close(self):
        """关闭数据库写入线程"""
        print(self.queue.qsize())
        self.queue.put(None)
        self.thread.join()
        self.conn.commit()
        return self.conn
