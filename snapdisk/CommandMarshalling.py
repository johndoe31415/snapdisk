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

import json
import struct
import collections

class MarshallingException(Exception): pass

class CommandMarshalling():
	_MAGIC = 4189080007
	_HEADER = struct.Struct("< L L Q")
	_HEADER_FIELDS = collections.namedtuple("Header", [ "magic", "msg_len", "payload_len" ])
	_MESSAGE = collections.namedtuple("Message", [ "msg", "payload" ])
	assert(_HEADER.size == 16)

	def __init__(self, recv_callback = None, send_callback = None):
		self._send_callback = send_callback
		self._recv_callback = recv_callback

	@classmethod
	def create_on_endpoint(cls, endpoint):
		return cls(send_callback = lambda data: endpoint.send(data), recv_callback = lambda length: endpoint.recv(length))

	def send_recv(self, msg = None, payload = None):
		self.send(msg = msg, payload = payload)
		recved = self.recv()
		if not isinstance(recved.msg, dict):
			raise MarshallingException("Invalid data type received: %s" % (type(recved.msg)))
		if not "status" in recved.msg:
			raise MarshallingException("Received response message contains no 'status' key.")
		if recved.msg["status"] != "ok":
			raise MarshallingException("Received response message contains error status code: %s (%s)" % (recved.msg["status"], recved.msg.get("text")))
		return recved

	def send(self, msg = None, payload = None):
		for chunk in self.marshal(msg, payload):
			self._send_callback(chunk)

	def recv(self):
		header_bin = self._recv_callback(self._HEADER.size)
		try:
			header = self._HEADER_FIELDS(*self._HEADER.unpack(header_bin))
		except struct.error as e:
			raise MarshallingException("Marshalling unpacking error: %s" % (str(e)))
		if header.magic != self._MAGIC:
			raise MarshallingException("Invalid magic number received (expected %08x but got %08x)." % (self._MAGIC, header.magic))
		msg_bin = self._recv_callback(header.msg_len)
		payload_bin = self._recv_callback(header.payload_len)
		msg = json.loads(msg_bin.decode("ascii"))
		return self._MESSAGE(msg = msg, payload = payload_bin)

	def marshal(self, msg = None, payload = None):
		msg_binary = json.dumps(msg, separators = (",", ":")).encode("ascii")
		if payload is None:
			payload = bytes()
		header = self._HEADER.pack(self._MAGIC, len(msg_binary), len(payload))
		yield header
		yield msg_binary
		yield payload

if __name__ == "__main__":
	import io

	buf = io.BytesIO()

	cmd = CommandMarshalling(send_callback = lambda data: buf.write(data), recv_callback = lambda length: buf.read(length))
	cmd.send({ "foo": "bar" })
	cmd.send({ "msg2": "moo", "koo": 123454 }, b"TEST")
	buf.seek(0)
	print(cmd.recv())
	print(cmd.recv())
