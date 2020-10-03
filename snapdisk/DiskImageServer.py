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

from .CommandMarshalling import CommandMarshalling, MarshallingException

class CommandException(Exception): pass
class CommandQuit(Exception): pass

class DiskImageServer():
	def __init__(self, image, endpoint, max_chunk_size):
		self._image = image
		self._endpoint = endpoint
		self._marshal = CommandMarshalling.create_on_endpoint(self._endpoint)
		self._chunk = None
		self._chunk_length = None
		self._chunk_offset = None
		self._max_chunk_size = max_chunk_size

	def _read_chunk(self, offset, length):
		if length > self._max_chunk_size:
			raise CommandException("Server chunk size limited at %d bytes, but %d bytes requested." % (self._max_chunk_size, length))
		if (self._chunk_offset != offset) or (self._chunk_length != length):
			self._chunk_offset = offset
			self._chunk_length = length
			self._chunk = self._image.get_chunk_at(self._chunk_offset, self._chunk_length)

	def _cmd_get_image_metadata(self, request):
		return {
			"device_name":		self._image.device_name,
			"disk_size":		self._image.disk_size,
		}

	def _cmd_get_chunk_hash(self, request):
		if not "offset" in request.msg:
			raise CommandException("Excpected marshalled data to contain 'offset' key.")
		if not "length" in request.msg:
			raise CommandException("Excpected marshalled data to contain 'length' key.")
		self._read_chunk(request.msg["offset"], request.msg["length"])
		return {
			"offset":		self._chunk_offset,
			"hash":			self._chunk.hash_value,
			"size":			len(self._chunk),
		}

	def _cmd_get_chunk_data(self, request):
		if not "offset" in request.msg:
			raise CommandException("Excpected marshalled data to contain 'offset' key.")
		if not "length" in request.msg:
			raise CommandException("Excpected marshalled data to contain 'length' key.")
		self._read_chunk(request.msg["offset"], request.msg["length"])
		return ({
			"offset":		self._chunk_offset,
			"hash":			self._chunk.hash_value,
		}, self._chunk.data)

	def _cmd_quit(self, request):
		raise CommandQuit("Connection closed successully.")

	def _process_command(self, request):
		if not isinstance(request.msg, dict):
			raise CommandException("Excpected marshalled data to be of type dict, but was %s." % (type(request)))
		if not "cmd" in request.msg:
			raise CommandException("Excpected marshalled data to contain 'cmd' key.")
		cmd_name = request.msg["cmd"]
		cmd_handler_name = "_cmd_" + cmd_name
		cmd_handler = getattr(self, cmd_handler_name, None)
		if cmd_handler is None:
			raise CommandException("No command handler for command '%s'." % (cmd_handler_name))
		return cmd_handler(request)

	def run(self):
		while True:
			try:
				request = self._marshal.recv()
				response = self._process_command(request)
			except (CommandException, MarshallingException) as e:
				self._marshal.send({ "status": "error", "text": str(e) })
				continue
			except CommandQuit as e:
				self._marshal.send({ "status": "ok", "text": str(e) })
				break

			if isinstance(response, tuple):
				(response_msg, response_payload) = response
			else:
				(response_msg, response_payload) = (response, None)
			if response_msg is None:
				response_msg = { }
			response_msg["status"] = "ok"
			self._marshal.send(response_msg, response_payload)
