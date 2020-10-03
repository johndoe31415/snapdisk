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
import hashlib
import contextlib
import subprocess

class GenericChunk():
	@property
	def hash_value(self):
		return self._hash_value

	def _chunk_target_dir(self, target_dir):
		return "%s/chunks/%s" % (target_dir, self.hash_value[:2])

	def already_stored(self, target_dir):
		dir_name = self._chunk_target_dir(target_dir)
		return os.path.isfile("%s/%s" % (dir_name, self.hash_value)) or os.path.isfile("%s/%s.gz" % (dir_name, self.hash_value))

	def store(self, target_dir, compression = None):
		dir_name = "%s/chunks/%s" % (target_dir, self.hash_value[:2])
		with contextlib.suppress(FileExistsError):
			os.makedirs(dir_name)
		_ = self.data		# Assure that chunk is fetched entirely if remote
		if compression is None:
			file_name = "%s/%s" % (dir_name, self.hash_value)
			with open(file_name, "wb") as f:
				f.write(self.data)
			return len(self.data)
		elif compression == "gz":
			file_name = "%s/%s.gz" % (dir_name, self.hash_value)
			with open(file_name, "wb") as f:
				proc = subprocess.Popen([ "pigz" ], stdout = f, stdin = subprocess.PIPE)
				proc.communicate(self.data)
			return os.stat(file_name).st_size
		else:
			raise NotImplementedError(compression)

class Chunk(GenericChunk):
	def __init__(self, data, hash_value = None):
		GenericChunk.__init__(self)
		assert(isinstance(data, bytes) or isinstance(data, bytearray))
		self._data = data
		if hash_value is not None:
			self._hash_value = hash_value
		else:
			self._hash_value = hashlib.sha384(self._data).hexdigest()

	@property
	def data(self):
		return self._data

	def __len__(self):
		return len(self._data)

class RemoteChunk(GenericChunk):
	def __init__(self, hash_value, size, retrieval_callback):
		GenericChunk.__init__(self)
		self._hash_value = hash_value
		self._size = size
		self._retrieval_callback = retrieval_callback
		self._chunk = None

	@property
	def data(self):
		if self._chunk is None:
			self._chunk = self._retrieval_callback()
			assert(self._chunk.hash_value == self.hash_value)
		return self._chunk.data

	def __len__(self):
		return self._size
