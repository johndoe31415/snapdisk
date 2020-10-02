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

import os
from .Chunk import Chunk, RemoteChunk
from .Endpoints import EndpointDefinition, SubprocessEndpoint
from .CommandMarshalling import CommandMarshalling

class GenericDiskImage():
	def __init__(self, device_name, chunk_size, disk_size):
		self._device_name = device_name
		self._chunk_size = chunk_size
		self._disk_size = disk_size
		self._chunk_count = (self._disk_size + self._chunk_size - 1) // self._chunk_size

	@property
	def device_name(self):
		return self._device_name

	@property
	def chunk_size(self):
		return self._chunk_size

	@property
	def disk_size(self):
		return self._disk_size

	@property
	def chunk_count(self):
		return self._chunk_count

	def iter_chunk_indices(self, start_offset = None):
		if start_offset is None:
			start_offset = 0
		else:
			assert((start_offset % self._chunk_size) == 0)

		return range(start_offset // self._chunk_size, self.chunk_count)

class DiskImage(GenericDiskImage):
	def __init__(self, device_name, chunk_size):
		GenericDiskImage.__init__(self, device_name = device_name, chunk_size = chunk_size, disk_size = self._get_disksize(device_name))
		self._f = None

	@staticmethod
	def _get_disksize(device_name):
		with open(device_name, "rb") as f:
			f.seek(0, os.SEEK_END)
			return f.tell()

	def __enter__(self):
		self._f = open(self._device_name, "rb")
		return self

	def __exit__(self, *args):
		self._f.close()
		self._f = None

	def get_chunk_at(self, offset):
		assert((offset % self._chunk_size) == 0)
		chunk_no = offset // self._chunk_size
		self._f.seek(offset)
		data = self._f.read(self._chunk_size)
		assert((len(data) == self._chunk_size) or (chunk_no == self.chunk_count - 1))
		return Chunk(data = data)

	def iter_chunks(self, start_offset = None):
		self._f.seek(start_offset)
		for chunk_no in self.iter_chunk_indices(start_offset):
			yield self.get_chunk_at(chunk_no * self._chunk_size)

class RemoteDiskImage(GenericDiskImage):
	def __init__(self, parsed_uri, chunk_size):
		self._parsed_uri = parsed_uri
		if parsed_uri.scheme == "ssh":
			username_hostname_port = parsed_uri.netloc
			if ":" in username_hostname_port:
				(username_hostname, port) = username_hostname_port.split(":", maxsplit = 1)
				port = int(port)
			else:
				username_hostname = username_hostname_port
				port = 22
			remote_filename = parsed_uri.path[1:]
			remote_command = [ "snapdisk.py", "serve", remote_filename ]
			command = [ "ssh", "-p", str(port), username_hostname, " ".join(remote_command) ]
			self._endpoint = SubprocessEndpoint(command)
		else:
			endpoint_definition = EndpointDefinition.from_parsed_uri(self._parsed_uri)
			self._endpoint = endpoint_definition.create_connection()
		self._marshal = CommandMarshalling.create_on_endpoint(self._endpoint)

		meta_data = self._marshal.send_recv({ "cmd": "get_image_metadata" })
		assert(meta_data.msg["chunk_size"] == chunk_size)
		GenericDiskImage.__init__(self, device_name = meta_data.msg["device_name"], chunk_size = chunk_size, disk_size = meta_data.msg["disk_size"])

	def __enter__(self):
		return self

	def __exit__(self, *args):
		self._marshal.send_recv({ "cmd": "quit" })

	def iter_chunks(self, start_offset = None):
		for chunk_no in range(start_offset // self._chunk_size, self.chunk_count):
			offset = chunk_no * self._chunk_size
			chunk_hash_msg = self._marshal.send_recv({ "cmd": "get_chunk_hash", "offset": offset })
			def _retrieve():
				chunk_data_msg = self._marshal.send_recv({ "cmd": "get_chunk_data", "offset": offset })
				assert((len(chunk_data_msg.payload) == self._chunk_size) or (chunk_no == self.chunk_count - 1))
				return Chunk(data = chunk_data_msg.payload)
			yield RemoteChunk(chunk_hash_msg.msg["hash"], chunk_hash_msg.msg["size"], retrieval_callback = _retrieve)
