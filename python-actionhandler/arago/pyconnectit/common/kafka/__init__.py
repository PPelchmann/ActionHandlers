import gevent
from gevent import monkey; monkey.patch_all()
from gevent.pool import Pool
from kafka import KafkaConsumer
import ujson as json

class SDFConsumer(object):
	def __init__(self, bootstrap_servers=['localhost:9092'], topics=[], handlers=[]):
		self.consumer = KafkaConsumer(
			*topics,
			group_id="netcool-adapter",
			auto_offset_reset='earliest',
			enable_auto_commit=True,
			key_deserializer=int,
			value_deserializer=lambda m: json.loads(m.decode('ascii')),
			bootstrap_servers=bootstrap_servers)
		self.handlers=handlers
		self.pool = Pool(size=5)
		self.loop = gevent.Greenlet(self.do_read)

	def start(self):
		self.loop.start()

	def halt(self):
		self.loop.kill()

	def serve_forever(self):
		self.loop.start()
		self.loop.join()

	def do_read(self):
		for message in self.consumer:
			self.pool.spawn(self.do_handle, message)

	def do_handle(self, message):
		for handler in self.handlers:
			handler.handle_message(message)
