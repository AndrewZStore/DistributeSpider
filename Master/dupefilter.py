from Master.Until.tool import redis_server
from Master.Until.bloomfilter import BloomFilter
import time


class DupeFilter(object):

    def __init__(self, request_queue, new_request_queue, dupefilter_queue):
        self.server = redis_server()
        self.rq = request_queue
        self.nrq = new_request_queue
        self.bf = BloomFilter(self.server, dupefilter_queue)

    @classmethod
    def from_settings(cls, settings):
        return cls(request_queue=settings.get('MASTER_REQUEST_QUEUE'),
                   new_request_queue=settings.get('MASTER_NEW_REQUEST_QUEUE'),
                   dupefilter_queue=settings.get('MASTER_DEPUFILTER_QUEUE'))

    def get_request(self):
        print('从新请求队列拿出请求')
        with self.server.pipeline(transaction=False) as pipe:
            pipe.zrange(self.nrq, 0, 0, withscores=True).zremrangebyrank(self.nrq, 0, 0)
            results, count = pipe.execute()
        if results:
            req = str(results[0][0], encoding='utf-8')
            score = results[0][1]

            return req, score
        else:
            return None, None

    def filter(self):
        req, score = self.get_request()
        if req and not self.bf.exists(req):
            self.server.zadd(self.rq, {req: score})
            self.bf.insert(req)

    def __len__(self):
        return self.server.zcard(self.nrq)

    def start_dupefilter(self):
        while True:
            if self.__len__() > 0:
                self.filter()
            else:
                time.sleep(3)


if __name__ == '__main__':
    d = DupeFilter.from_settings()
    d.start_dupefilter()




