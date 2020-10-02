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
import contextlib
import datetime
import json
import enum

class SnapshotWriterException(Exception): pass

class SnapshotMode(enum.Enum):
	Create = "create"
	Resume = "resume"
	Overwrite = "overwrite"

class SnapshotWriter():
	def __init__(self, image, target, name, compression = None, mode = SnapshotMode.Create):
		assert(isinstance(mode, SnapshotMode))
		self._image = image
		self._target = target
		self._name = name
		self._compression = compression
		with contextlib.suppress(FileExistsError):
			os.makedirs(self._target)
		self._chunks = [ ]
		self._start_ts = datetime.datetime.utcnow()
		self._end_ts = self._start_ts
		self._total_bytes_appended = 0
		self._chunks_deduplicated = 0
		self._chunks_deduplicated_size = 0
		self._chunks_stored = 0
		self._chunks_stored_size = 0
		if (mode == SnapshotMode.Create) and os.path.isfile(self.snapshot_filename):
			raise SnapshotWriterException("Refusing to overwrite already existing snapshot file: %s" % (self.snapshot_filename))
		elif (mode == SnapshotMode.Resume):
			self._load_snapshot()

	def _load_snapshot(self):
		if not os.path.isfile(self.snapshot_filename):
			raise SnapshotWriterException("Cannot resume non-existent snapshot file: %s" % (self.snapshot_filename))
		with open(self.snapshot_filename) as f:
			snapshot_meta = json.load(f)
		if snapshot_meta["meta"]["disk_size"] != self._image.disk_size:
			raise SnapshotWriterException("Disk size in snapshot %s is %d bytes, but trying to resume disk with size %d bytes." % (self.snapshot_filename, snapshot_meta["meta"]["disk_size"], self._image.disk_size))
		if snapshot_meta["meta"]["chunk_size"] != self._image.chunk_size:
			raise SnapshotWriterException("Chunk size in snapshot %s is %d bytes, but trying to resume with chunk size %d bytes." % (self.snapshot_filename, snapshot_meta["meta"]["chunk_size"], self._image.chunk_size))
		self._start_ts = datetime.datetime.strptime(snapshot_meta["meta"]["start_ts"], "%Y-%m-%dT%H:%M:%SZ")
		self._end_ts = datetime.datetime.utcnow()
		self._chunks = snapshot_meta["chunks"]

	@property
	def position(self):
		pos = len(self._chunks) * self._image.chunk_size
		if pos > self._image.disk_size:
			pos = self._image.disk_size
		return pos

	@property
	def total_bytes_appended(self):
		return self._total_bytes_appended

	@property
	def chunks_deduplicated(self):
		return self._chunks_deduplicated

	@property
	def chunks_deduplicated_size(self):
		return self._chunks_deduplicated_size

	@property
	def chunks_stored(self):
		return self._chunks_stored

	@property
	def chunks_stored_size(self):
		return self._chunks_stored_size

	@property
	def snapshot_filename(self):
		snapshot_filename = self._target + "/" + self._name + ".json"
		return snapshot_filename

	def _append_chunk(self, chunk):
		self._total_bytes_appended += len(chunk)
		self._end_ts = datetime.datetime.utcnow()
		if chunk.already_stored(self._target):
			self._chunks_deduplicated += 1
			self._chunks_deduplicated_size += len(chunk)
		else:
			self._chunks_stored += 1
			self._chunks_stored_size += chunk.store(self._target, compression = self._compression)
		self._chunks.append(chunk.hash_value)

	def commit(self):
		history = {
			"meta": {
				"target":			self._target,
				"name":				self._name,
				"disk_size":		self._image.disk_size,
				"chunk_count":		self._image.chunk_count,
				"chunk_size":		self._image.chunk_size,
				"device_name":		self._image.device_name,
				"start_ts":			self._start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
				"end_ts":			self._end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
				"version":			1,
			},
			"chunks": self._chunks,
		}
		with open(self.snapshot_filename, "w") as f:
			json.dump(history, fp = f)

	def _iter_chunks(self):
		yield from self._image.iter_chunks(start_offset = self.position)

	def create(self, progress_callback = None, progress_callback_period = None):
		last_progress_update = self.total_bytes_appended
		for chunk in self._iter_chunks():
			self._append_chunk(chunk)

			progress_since_last_callback = self.total_bytes_appended - last_progress_update
			if (progress_callback is not None) and (progress_callback_period is not None) and (progress_since_last_callback >= progress_callback_period):
				last_progress_update = self.total_bytes_appended
				progress_callback(self)
		if progress_callback is not None:
			progress_callback(self)

	def __enter__(self):
		return self

	def __exit__(self, *args):
		self.commit()
