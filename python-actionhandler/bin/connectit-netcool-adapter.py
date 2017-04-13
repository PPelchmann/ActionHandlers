#!/usr/bin/env python
"""connectit-netcool-adapter

Usage:
  connectit-netcool-adapter [options] (start|stop|restart)

Options:
  --debug            do not run as daemon and log to stderr
  --pidfile=PIDFILE  Specify pid file [default: /var/run/{progname}.pid]
  -h --help          Show this help screen
"""
import gevent
from gevent import monkey; monkey.patch_all(sys=True)
import logging, logging.config, sys, os, signal, gevent.hub, gevent.queue, gevent.pywsgi
from configparser import ConfigParser
from docopt import docopt
from urllib.parse import urlparse, urlunparse

from arago.common.daemon import daemon as Daemon
from arago.common.logging.logger import Logger

from arago.pyconnectit.common.rest import RESTAPI

from arago.pyconnectit.connectors.common.trigger import FastTrigger
from arago.pyconnectit.connectors.common.handlers.log_status_change import LogStatusChange
from arago.pyconnectit.connectors.common.handlers.log_comments import LogComments
from arago.pyconnectit.connectors.netcool.handlers.sync_netcool_status import BatchSyncNetcoolStatus
from arago.pyconnectit.common.delta_store import DeltaStore

class ConnectitDaemon(Daemon):
	def run(self):
		config_path = '/opt/autopilot/connectit/conf/'
		main_config_file = os.path.join(
			config_path, 'connectit-netcool-adapter.conf')
		environments_config_file = os.path.join(
			config_path, 'connectit-netcool-adapter-environments.conf')
		share_dir = os.path.join(
			os.getenv('PYTHON_DATADIR'), 'connectit-netcool-adapter')

		# Read config files

		#logger.info("Reading config file {file}".format(file=main_config_file))
		adapter_config=ConfigParser()
		adapter_config.read(main_config_file)

		#logger.info("Reading config file {file}".format(file=environments_config_file))
		environments_config=ConfigParser()
		environments_config.read(environments_config_file)

		# Setup logging in normal operation

		logging.setLoggerClass(Logger)
		logger = logging.getLogger('root')
		level = getattr(
			logger, adapter_config.get(
				'Logging', 'loglevel',
				fallback='VERBOSE'))
		logfile = adapter_config.get(
			'Logging', 'logfile',
			fallback=os.path.join(
				'/var/log/autopilot/connectit/',
				'netcool-adapter.log'))
		debuglevel = getattr(
			logger, adapter_config.get(
				'Logging', 'debuglevel',
				fallback='TRACE'))
		logger.setLevel(level)

		logfile_formatter = logging.Formatter(
			"%(asctime)s [%(levelname)s] %(message)s",
			"%Y-%m-%d %H:%M:%S")
		try:
			logfile_handler = logging.FileHandler(logfile)
		except PermissionError as e:
			print(e, file=sys.stderr, flush=True)
			sys.exit(5)
		logfile_handler.setFormatter(logfile_formatter)
		logfile_handler.setLevel(level)
		logger.addHandler(logfile_handler)

		# Setup debug logging
		if self.debug:
			stream_handler = logging.StreamHandler()
			stream_handler.setLevel(debuglevel)
			debug_formatter = logging.Formatter(
				"[%(levelname)s] %(message)s")
			stream_handler.setFormatter(debug_formatter)
			logger.setLevel(debuglevel)

			logger.addHandler(stream_handler)
			logger.info("DEBUG MODE: Logging to console and logfile")

		# Configure DeltaStore
		try:
			os.makedirs(
				adapter_config['DeltaStore']['data_dir'],
				mode=0o700, exist_ok=True)
		except OSError as e:
			logger.critical("Can't create data directory: " + e)
			sys.exit(5)
		delta_store_map= {
			env:DeltaStore(
				db_path = os.path.join(
					adapter_config['DeltaStore']['data_dir'], env),
				max_size = 1024 * 1024 * adapter_config.getint(
					'DeltaStore', 'max_size_in_mb', fallback=1024),
				schemafile = open(
					environments_config[env]['event_schema']))
			for env
			in environments_config.sections()
		}

		# Configure Triggers and Handlers

		triggers= [
			FastTrigger(
				open(os.path.join(
					share_dir, "schemas/event-status-change.json")),
				[LogStatusChange(),
				 BatchSyncNetcoolStatus.from_config(
					 adapter_config,
					 environments_config,
					 delta_store_map=delta_store_map)]),
			FastTrigger(
				open(os.path.join(
					share_dir, "schemas/event-comment-added.json")),
				[LogComments()])
		]

		# Setup HTTP server for REST API

		rest_url = urlparse(
			adapter_config.get('RESTInterface', 'base_url'))

		server = gevent.pywsgi.WSGIServer(
			(rest_url.hostname, rest_url.port),
			RESTAPI.from_config(
				adapter_config=adapter_config,
				environments_config=environments_config,
				delta_store_map=delta_store_map,
				triggers=triggers
			).app,
			log=None,
			error_log=logger)

		# Handle graceful shutdown

		def exit_gracefully():
			logger.info("Shutting down ...")
			server.stop()
			logger.debug("Shutdown complete!")
			gevent.idle()
			sys.exit(0)

		gevent.hub.signal(signal.SIGINT, exit_gracefully)
		gevent.hub.signal(signal.SIGTERM, exit_gracefully)

		# Start

		logger.info("Starting REST service at {url}".format(
			url=urlunparse(rest_url)))
		server.serve_forever()


if __name__ == "__main__":
	args=docopt(__doc__, version='connectit-netcool-adapter 0.2')
	daemon = ConnectitDaemon(args['--pidfile'], debug=args['--debug'])
	if   args['start']:
		daemon.start()
	elif args['stop']:
		daemon.stop()
	elif args['restart']:
		daemon.restart()
	sys.exit(0)