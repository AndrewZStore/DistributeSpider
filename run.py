from .Scheduler import MasterScheduler
from .Until.tool import settings


m = MasterScheduler.from_settings(settings)
m._init_request_queue()

