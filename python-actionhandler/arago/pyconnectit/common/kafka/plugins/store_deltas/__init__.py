import logging
from arago.pyconnectit.common.delta_store import DeltaStoreFull

class StoreDeltas(object):
	def __init__(self, topic_map, delta_store_map):
		self.logger = logging.getLogger('root')
		self.topic_map = topic_map
		self.delta_store_map = delta_store_map
	def handle_message(self, message):
		print("StoreDeltas")
		print(message)
		if message.topic in self.topic_map.keys():
			try:
				self.logger.debug("Storing delta in {store}".format(
					store=self.delta_store_map[self.topic_map[message.topic]]))
				self.delta_store_map[self.topic_map[message.topic]].append(
					message.value['mand']['eventId'],
					message.value
				)
			except DeltaStoreFull as e:
				self.logger.critical("DeltaStore for {env} can't store this delta: {err}".format(env=self.topic_map[message.topic], err=e))
				raise falcon.HTTPInsufficientStorage(title="DeltaStore full", description="")
			except KeyError as e:
				print(e)
				self.logger.warning(
					"No DeltaStore defined for environment: {env}".format(
						env=self.topic_map[message.topic]))
