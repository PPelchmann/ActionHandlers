import sys
import argparse
import base64
import winrm
import re
import xml.etree.ElementTree as ET
import codecs
import xmltodict

class certSession(winrm.Session):
    def __init__(self, endpoint, transport, cert, key, validation='ignore'):
        self.protocol = winrm.Protocol(
            endpoint=endpoint,
            transport=transport,
            cert_pem=cert,
            cert_key_pem=key,
            server_cert_validation='ignore'
        )

    def run_ps(self, script):
        """base64 encodes a Powershell script and executes the powershell
        encoded script command
        """

        # must use utf16 little endian on windows
        base64_script = base64.b64encode(script.encode("utf_16_le"))
        rs = self.run_cmd("mode con: cols=1024 & powershell -encodedcommand %s" % (base64_script))
        if len(rs.std_err):
            # if there was an error message, clean it it up and make it human
            # readable
            rs.std_err = self.clean_error_msg(rs.std_err)
        return rs

class Script(object):
    psWrapper="""\
$t = [IO.Path]::GetTempFileName()
[System.Text.Encoding]::Unicode.GetString([System.Convert]::FromBase64String("{script}")) >$t
gc $t | powershell - 2>&1 | %{{$e=@("psout","pserr")[[byte]($_.GetType().Name -eq "ErrorRecord")];return "<$e><![CDATA[$_]]></$e>"}}
rm $t
exit $LastExitCode
"""
    cmdWrapper="""\
$t = [IO.Path]::GetTempFileName() | ren -NewName {{ $_ -replace 'tmp$', 'bat' }} -PassThru
[System.Text.Encoding]::Unicode.GetString([System.Convert]::FromBase64String("{script}")) | out-file -encoding "ASCII" $t
& cmd.exe /q /c $t 2>&1 | %{{$e=@("psout","pserr")[[byte]($_.GetType().Name -eq "ErrorRecord")];return "<$e><![CDATA[$_]]></$e>"}}
rm $t
exit $LastExitCode
"""
    def __init__(self, script, interpreter):
        if interpreter: self.setInterpreter(interpreter) 
        if script: self.setScript(script)
        self.setResult()
            
    def setInterpreter(self, interpreter):
        self.interpreter=interpreter
        if interpreter == 'cmd': self.wrapper=self.cmdWrapper
        elif interpreter == 'powershell': self.wrapper=self.psWrapper

    def setScript(self, script):
        if self.interpreter: self.script=self.prep_script(base64.b64encode(script.encode("utf_16_le")))
        else: print >>sys.stderr, "Error: You have to set the interpreter, first!"

    def setResult(self, rs=None):
        self.rs=rs

    def run(self, Session):
        self.rs=Session.run_ps(self.script)
    
    def prep_script(self, raw_script):
        return self.wrapper.format(script=raw_script)

    def print_output(self):
        xml = "<root>\n" + self.rs.std_out.decode('cp850') + "</root>"
        root = ET.fromstring(xml.encode('utf8'))
        nodes = root.findall("./*")
        for s in nodes:
            if s.text:
                s.text = s.text.rstrip("\n ")
                if s.tag == 'pserr':
                    print >>sys.stderr, s.text
                elif s.tag == 'psout':
                    print >>sys.stdout, s.text

### MAIN ###

sys.stdout = codecs.getwriter('utf8')(sys.stdout)

parser = argparse.ArgumentParser()

parser.add_argument("script",
                    help="MANDATORY: path to a file containing the commands",
                    type=argparse.FileType('r'))
parser.add_argument("-H", "--hostname",
                    help="MANDATORY: the hostname of the machine to execute the command on",
                    required=True)
parser.add_argument("-p", "--port",
                    help="the port WinRM is listening on on the target machine (default=5986)",
                    type=int, default=5986)
parser.add_argument("-t", "--transport",
                    help="the transport protocol in use (default=ssl), only ssl implemented by now",
                    choices=['kerberos', 'ssl', 'plaintext'], default='ssl')
parser.add_argument("-c", "--certificate",
                    help="MANDATORY: path to the file containing the client certificate",
                    required=True, type=argparse.FileType('r'))
parser.add_argument("-k", "--keyfile",
                    help="MANDATORY: path to the file containing the client certificate's private key",
                    required=True, type=argparse.FileType('r'))
parser.add_argument("-i", "--interpreter",
                    help="the command interpreter to use, either cmd or powershell (default)",
                    choices=['cmd', 'powershell'], default='powershell')

args = parser.parse_args()


mySession = certSession(
            endpoint="https://{hostname}:{port}/wsman".format(hostname=args.hostname, port=args.port),
            transport=args.transport,
            cert=args.certificate.name,
            key=args.keyfile.name,
            validation='ignore')

myScript=Script(script=args.script.read(),
                interpreter=args.interpreter)
myScript.run(mySession)
myScript.print_output()
sys.exit(myScript.rs.status_code or 0)
