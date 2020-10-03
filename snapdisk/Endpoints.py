#	snapdisk - User-mode block device snapshotting utility
#	Copyright (C) 2020-2020 Johannes Bauer
#
#	This file is part of snapdisk.
#
#	snapdisk is free software; you can redistribute it and/or modify
#	it under the terms of the GNU General Public License as published by
#	the Free Software Foundation; this program is ONLY licensed under
#	version 3 of the License, later versions are explicitly excluded.
#
#	snapdisk is distributed in the hope that it will be useful,
#	but WITHOUT ANY WARRANTY; without even the implied warranty of
#	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#	GNU General Public License for more details.
#
#	You should have received a copy of the GNU General Public License
#	along with snapdisk; if not, write to the Free Software
#	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#	Johannes Bauer <JohannesBauer@gmx.de>

import sys
import socket
import os
import ssl
import contextlib
import subprocess
import urllib.parse
import tempfile
from .Certificates import Certificates

class EndpointTerminatedException(Exception): pass

class ReliableEndpoint():
	def send(self, data):
		bytes_sent = 0
		while bytes_sent < len(data):
			bytes_sent += self._send(data[bytes_sent : ])

	def recv(self, length):
		received = bytearray()
		while len(received) < length:
			missing = length - len(received)
			chunk = self._recv(missing)
			if len(chunk) == 0:
				raise EndpointTerminatedException("Received zero bytes; connection severed.")
			received += chunk
		return received

class StdinStdoutEndpoint(ReliableEndpoint):
	def __init__(self):
		pass

	def _send(self, data):
		written = sys.stdout.buffer.write(data)
		sys.stdout.buffer.flush()
		return written

	def _recv(self, length):
		return sys.stdin.buffer.read(length)

class SocketEndpoint(ReliableEndpoint):
	def __init__(self, sock):
		self._sock = sock

	@property
	def sock(self):
		return self._sock

	def _send(self, data):
		return self._sock.send(data)

	def _recv(self, length):
		return self._sock.recv(length)

	@classmethod
	def _prepare_ip_socket(cls, bind_address, bind_port):
		sock = socket.socket()
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((bind_address, bind_port))
		sock.listen(1)
		return sock

	@classmethod
	def create_ip_listener(cls, bind_address, bind_port):
		sock = cls._prepare_ip_socket(bind_address, bind_port)
		(conn, peer) = sock.accept()
		return cls(conn)

	@classmethod
	def create_ip_connection(cls, connect_address, connect_port):
		conn = socket.create_connection((connect_address, connect_port))
		return cls(conn)

	@classmethod
	def _create_tls_context(cls, keyfile, server = True):
		cert_key = Certificates.load_cert_key(keyfile)
		tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT)
		tls_context.minimum_version = ssl.TLSVersion.TLSv1_2
		tls_context.maximum_version = ssl.TLSVersion.TLSv1_2
		tls_context.verify_mode = ssl.CERT_REQUIRED
		tls_context.check_hostname = False
		with tempfile.NamedTemporaryFile("w") as cert_file, tempfile.NamedTemporaryFile("w") as key_file:
			cert_file.write(cert_key.cert)
			key_file.write(cert_key.key)
			cert_file.flush()
			key_file.flush()
			tls_context.load_cert_chain(cert_file.name, key_file.name)
		with tempfile.NamedTemporaryFile("w") as trusted_peer_file:
			trusted_peer_file.write("\n".join(cert_key.trusted_peer_certs))
			trusted_peer_file.flush()
			tls_context.load_verify_locations(cafile = trusted_peer_file.name)
		tls_context.set_ciphers("ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256")
		return tls_context

	@classmethod
	def create_tls_listener(cls, bind_address, bind_port, keyfile):
		sock = cls._prepare_ip_socket(bind_address, bind_port)
		tls_context = cls._create_tls_context(keyfile, server = True)
		tls_sock = tls_context.wrap_socket(sock, server_side = True)
		while True:
			try:
				(conn, peer) = tls_sock.accept()
			except (ssl.SSLError, OSError) as e:
				print("Connection of client rejected: %s - %s" % (e.__class__.__name__, str(e)))
				continue
			return cls(conn)

	@classmethod
	def create_tls_connection(cls, connect_address, connect_port, keyfile):
		conn = socket.create_connection((connect_address, connect_port))
		tls_context = cls._create_tls_context(keyfile, server = False)
		tls_sock = tls_context.wrap_socket(conn, server_side = False)
		return cls(tls_sock)

	@classmethod
	def create_unix_listener(cls, bind_filename):
		with contextlib.suppress(FileNotFoundError):
			os.unlink(bind_filename)
		sock = socket.socket(family = socket.AF_UNIX)
		sock.bind(bind_filename)
		sock.listen(1)
		(conn, peer) = sock.accept()
		return cls(conn)

	@classmethod
	def create_unix_connection(cls, connect_filename):
		sock = socket.socket(family = socket.AF_UNIX)
		sock.connect(connect_filename)
		return cls(sock)

class SubprocessEndpoint(ReliableEndpoint):
	def __init__(self, cmd):
		self._cmd = cmd
		self._proc = subprocess.Popen(self._cmd, stdin = subprocess.PIPE, stdout = subprocess.PIPE)

	def _send(self, data):
		written = self._proc.stdin.write(data)
		self._proc.stdin.flush()
		return written

	def _recv(self, length):
		return self._proc.stdout.read(length)


class EndpointDefinition():
	def __init__(self, scheme, variables = None):
		self._scheme = scheme
		self._variables = variables

	@property
	def scheme(self):
		return self._scheme

	def __getitem__(self, key):
		return self._variables[key]

	def create_listener(self):
		if self.scheme == "stdout":
			return StdinStdoutEndpoint()
		elif self.scheme == "ip":
			return SocketEndpoint.create_ip_listener(self["address"], self["port"])
		elif self.scheme == "tls":
			return SocketEndpoint.create_tls_listener(self["address"], self["port"], self["keyfile"])
		elif self.scheme == "unix":
			return SocketEndpoint.create_unix_listener(self["filename"])
		else:
			raise NotImplementedError(self.scheme)

	def create_connection(self):
		if self.scheme == "stdout":
			return StdinStdoutEndpoint()
		elif self.scheme == "ip":
			return SocketEndpoint.create_ip_connection(self["address"], self["port"])
		elif self.scheme == "tls":
			return SocketEndpoint.create_tls_connection(self["address"], self["port"], self["keyfile"])
		elif self.scheme == "unix":
			return SocketEndpoint.create_unix_connection(self["filename"])
		else:
			raise NotImplementedError(self.scheme)

	@classmethod
	def _netloc_get_address_port(cls, netloc, default_port, default_address):
		address_port = netloc.split(":", maxsplit = 1)
		if len(address_port) == 1:
			(address, port) = (address_port[0], default_port)
		else:
			(address, port) = (address_port[0], int(address_port[1]))
		if address == "":
			address = default_address
		return (address, port)

	@classmethod
	def from_parsed_uri(cls, parsed):
		if parsed.scheme == "stdout":
			return cls(parsed.scheme)
		elif parsed.scheme == "ip":
			(address, port) = cls._netloc_get_address_port(parsed.netloc, default_port = 55860, default_address = "127.0.0.1")
			return cls(parsed.scheme, variables = {
				"address":		address,
				"port":			port,
			})
		elif parsed.scheme == "unix":
			return cls(parsed.scheme, variables = {
				"filename":		parsed.netloc + parsed.path,
			})
		elif parsed.scheme == "tls":
			(address, port) = cls._netloc_get_address_port(parsed.netloc, default_port = 48748, default_address = "127.0.0.1")
			keyfile = parsed.path[1:]
			return cls(parsed.scheme, variables = {
				"address":		address,
				"port":			port,
				"keyfile":		keyfile,
			})
		else:
			raise NotImplementedError("Invalid scheme: %s" % (parsed.scheme))

	@classmethod
	def parse(cls, text):
		parsed = urllib.parse.urlparse(text)
		return cls.from_parsed_uri(parsed)

	def __str__(self):
		return "Endpoint<%s, %s>" % (self.scheme, str(self._variables))
