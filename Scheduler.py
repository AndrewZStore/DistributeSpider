from .Until.tool import redis_server, settings
from .dupefilter import DupeFilter
import time

# TODO 从redis数据库获取各爬虫节点的系统运行状况
# TODO 依据系统运行状况向各节点分发请求，直接将请求写入节点redis数据库

class MasterScheduler(object):

    def __init__(self, request_queue):
        self.rq = request_queue
        self.dupefilter = DupeFilter.from_settings(settings)

    @classmethod
    def from_settings(cls, settings):
        return cls(request_queue=settings.get('MASTER_REQUEST_QUEUE'))

    def _init_request_queue(self):
        start_urls = ['https://weibo.com/1730336902/info']
        self.server = redis_server()
        for url in start_urls:
            self.server.zadd(self.rq, -100, url)
        self.dupefilter.start_dupefilter()


if __name__ == '__main__':
    m = MasterScheduler.from_settings(settings)
    m._init_request_queue()


