import gevent
import tempfile

from pyactionhandler import Action
from pyactionhandler.winrm.session import certSession
from pyactionhandler.winrm.script import Script
from configparser import NoOptionError
from pyactionhandler.common.pmp import PMPSession, TokenAuth, PMPCredentials

import winrm.exceptions
import pyactionhandler.winrm.exceptions
import logging



class WinRMCmdAction(Action):
	def __init__(self, num, node, zmq_info, timeout, parameters,
				 pmp_config, jumpserver_config, ssl=True):
		super(WinRMCmdAction, self).__init__(
			num, node, zmq_info, timeout, parameters)
		self.logger = logging.getLogger('root')
		self.pmp_config=pmp_config
		self.jumpserver_config=jumpserver_config
		self.jumpserver = parameters['RemoteExecutionServer'] if 'RemoteExecutionServer' in parameters else None
		self.customer = parameters['CustomerID'] if 'CustomerID' in parameters else 'default'
		self.ssl=ssl

	def __str__(self):
		return "cmd.exe command '{cmd}' on '{node}'".format(
			cmd=self.parameters['Command'],
			node=self.parameters['Hostname'])

	@staticmethod
	def init_direct_session(host, port, protocol, auth):
		return certSession(
			endpoint="{protocol}://{hostname}:{port}/wsman".format(
				protocol=protocol,
				hostname=host,
				port=port),
			auth=auth)

	@staticmethod
	def init_jump_session(jump_host, jump_port, jump_protocol,
						  jump_auth, target_host, target_auth):
		return certSession(
			endpoint="{protocol}://{hostname}:{port}/wsman".format(
				protocol = jump_protocol,
				hostname = jump_host,
				port=jump_port),
			certificate=jump_auth,
			target=target_host,
			target_auth=target_auth)

	@staticmethod
	def init_pmp_session(pmp_endpoint, pmp_token):
		s = PMPSession(pmp_endpoint)
		s.auth=TokenAuth(pmp_token)
		s.verify=False
		return s

	@staticmethod
	def init_script(script):
		return Script(
			script=script,
			interpreter='cmd',
			cols=120)

	def pmp_get_credentials(self, pmp_session, resource, account):
		try:
			return PMPCredentials(
				pmp_session, ResourceName=resource, AccountName=account)
		except pyactionhandler.common.pmp.exceptions.PMPError:
			self.logger.error("[{anum}] Credentials not found in PMP, resource "
							  "'{res}', account '{acc}'".format(
								  anum=self.num,
								  res=self.parameters['Hostname'],
								  acc=self.parameters['ServiceAccount']))
			self.statusmsg="Credentials not found in PMP"
			raise

	def winrm_run_script(self, winrm_session):
		script=self.init_script(self.parameters['Command'])
		try:
			script.run(winrm_session)
			self.output, self.error_output = script.get_outputs()
			self.system_rc = script.rs.status_code
			self.success=True
		except (winrm.exceptions.WinRMError, winrm.exceptions.WinRMTransportError, pyactionhandler.winrm.exceptions.WinRMError) as e:
			self.statusmsg=str(e)
			self.logger.error("[{anum}] An error occured during command execution on {node}: {err}".format(anum=self.num, node=self.node,err=str(e)))

	def __call__(self):

		pmp_session=self.init_pmp_session(
			pmp_endpoint=self.pmp_config.get(self.customer, 'URL'),
			pmp_token=self.pmp_config.get(self.customer, 'Token'))

		try:
			target_auth=self.pmp_get_credentials(
				pmp_session=pmp_session,
				resource=self.parameters['PMPResource'],
				account=self.parameters['ServiceAccount'])
		except pyactionhandler.common.pmp.exceptions.PMPError:
			return

		if self.jumpserver:
			try:
				cert=self.pmp_get_credentials(
					pmp_session=pmp_session,
					resource=self.jumpserver_config.get(
						self.jumpserver,
						'PMP_Resource'
						),
					account=self.jumpserver_config.get(
						self.jumpserver,
						'PMP_WinRM_Account'
						)).ssl_cert
			except pyactionhandler.common.pmp.exceptions.PMPError as e:
				self.statusmsg="PMP Error: {msg}".format(e.message)
				return
			except NoOptionError:
				self.statusmsg="Jumpserver config for PMP is missing!"
				return
			with tempfile.NamedTemporaryFile() as cert_file:
				cert_file.write(cert)
				winrm_session=self.init_jump_session(
					jump_host=self.jumpserver,
					jump_protocol = 'https' if self.ssl else 'http',
					jump_port = '5986' if self.ssl else '5985',
					jump_auth = cert_file.name,
					target_host=self.parameters['Hostname'],
					target_auth=target_auth)
				self.logger.debug("[{anum}] Connecting to "
								  "'{target}' via Jumpserver '{jump}'".format(
									  anum=self.num,
									  target=self.parameters['Hostname'],
									  jump=self.jumpserver))
				self.winrm_run_script(winrm_session)
		else:
			winrm_session=self.init_direct_session(
				host = self.parameters['Hostname'],
				protocol = 'https' if self.ssl else 'http',
				port = '5986' if self.ssl else '5985',
				auth=target_auth)
			self.logger.debug("[{anum}] Connecting directly to '{target}'".format(anum=self.num, target=self.parameters['Hostname']))
			self.winrm_run_script(winrm_session)

class WinRMPowershellAction(WinRMCmdAction):
	def init_script(self,script):
		return Script(
			script=script,
			interpreter='ps',
			cols=120)

	def __str__(self):
		return "powershell.exe command '{cmd}' on '{node}'".format(
			cmd=self.parameters['Command'],
			node=self.parameters['Hostname'])
