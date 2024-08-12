import queue
import threading


def mapget(map, key):
    if key in map:
        return map[key]
    else:
        return 0
class DBWriter:
    def __init__(self, conn,precompiledsql, batch_size=1000):
        self.batch_size = batch_size
        self.sql=precompiledsql
        self.conn=conn
        self.queue = queue.Queue(-1)
        self.thread = threading.Thread(target=self._db_worker, daemon=True)
        self.thread.start()
        self.sent=0

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
                print("batch写入",len(batch))
                print("queue长度",self.queue.qsize())
                batch.clear()
        # 处理剩余的数据
        if batch:
            cursor.executemany(self.sql,batch)
            print("batch写入最后",len(batch))
    def add_data(self, data):
        """向队列中添加数据"""
        self.queue.put(data)

    def add_batch(self,batch):
        self.queue.put(batch)
    def close(self):
        """关闭数据库写入线程"""
        self.queue.put(None)
        self.thread.join()
        print(self.sent)
        self.conn.commit()
        return self.conn
