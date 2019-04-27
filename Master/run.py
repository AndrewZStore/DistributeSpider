from Master.Scheduler import MasterScheduler
from Master.Until.tool import settings


m = MasterScheduler.from_settings(settings)
m._init_request_queue()

if __name__ == '__main__':
    pass