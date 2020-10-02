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
import contextlib
import subprocess
import urllib.parse

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

	def _send(self, data):
		return self._sock.send(data)

	def _recv(self, length):
		return self._sock.recv(length)

	@classmethod
	def create_ip_listener(cls, bind_address, bind_port):
		sock = socket.socket()
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((bind_address, bind_port))
		sock.listen(1)
		(conn, peer) = sock.accept()
		return cls(conn)

	@classmethod
	def create_ip_connection(cls, connect_address, connect_port):
		conn = socket.connect((connect_address, connect_port))
		return cls(conn)

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
		elif self.scheme == "unix":
			return SocketEndpoint.create_unix_listener(self["filename"])
		else:
			raise NotImplementedError(self.scheme)

	def create_connection(self):
		if self.scheme == "stdout":
			return StdinStdoutEndpoint()
		elif self.scheme == "ip":
			return SocketEndpoint.create_ip_connection(self["address"], self["port"])
		elif self.scheme == "unix":
			return SocketEndpoint.create_unix_connection(self["filename"])
		else:
			raise NotImplementedError(self.scheme)

	@classmethod
	def from_parsed_uri(cls, parsed):
		if parsed.scheme == "stdout":
			return cls(parsed.scheme)
		elif parsed.scheme == "ip":
			address_port = parsed.netloc.split(":", maxsplit = 1)
			if len(address_port) == 1:
				(address, port) = (address_port[0], 55860)
			else:
				(address, port) = (address_port[0], int(address_port[1]))
			if address == "":
				address = "127.0.0.1"
			return cls(parsed.scheme, variables = {
				"address":		address,
				"port":			port,
			})
		elif parsed.scheme == "unix":
			return cls(parsed.scheme, variables = {
				"filename":		parsed.netloc + parsed.path,
			})
		else:
			raise NotImplementedError("Invalid scheme: %s" % (parsed.scheme))

	@classmethod
	def parse(cls, text):
		parsed = urllib.parse.urlparse(text)
		return cls.from_parsed_uri(parsed)

	def __str__(self):
		return "Endpoint<%s, %s>" % (self.scheme, str(self._variables))
