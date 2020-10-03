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

import time
import datetime
import urllib.parse
from .BaseAction import BaseAction
from .DiskImage import DiskImage, RemoteDiskImage
from .SnapshotWriter import SnapshotMode, SnapshotWriter
from .FilesizeFormatter import FilesizeFormatter
from .TimeFormatter import TimeFormatter

class ActionSnapshot(BaseAction):
	def _progress(self, writer):
		pos = writer.position
		disk_size = self._image.disk_size
		tdiff = time.time() - self._t0
		if tdiff < 1:
			progress = 0
			speed_str = "N/A"
		else:
			progress = writer.total_bytes_appended / tdiff
			speed_str = self._size_fmt(round(progress)) + "/s"
		print("%6.2f%%: %s of %s; %s deduplicated, %s stored. Runtime %s, speed %s." % (pos / disk_size * 100, self._size_fmt(pos), self._size_fmt(disk_size), self._size_fmt(writer.chunks_deduplicated_size), self._size_fmt(writer.chunks_stored_size), self._time_fmt(tdiff), speed_str))
		writer.commit()

	def run(self):
		self._t0 = time.time()
		self._time_fmt = TimeFormatter()
		self._size_fmt = FilesizeFormatter(base1000 = self._args.print_si_units)

		parsed_src = urllib.parse.urlparse(self._args.src)
		if parsed_src.scheme == "":
			# Local file is source
			self._image = DiskImage(self._args.src, chunk_size = self._args.chunk_size)
		else:
			# Some kind of endpoint was given.
			self._image = RemoteDiskImage(parsed_src, chunk_size = self._args.chunk_size, remote_snapdisk_binary = self._args.remote_snapdisk)

		if self._args.name is None:
			snapshot_name = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
		else:
			snapshot_name = self._args.name
		mode = SnapshotMode(self._args.mode)
		with self._image, SnapshotWriter(image = self._image, target = self._args.dst, name = snapshot_name, compression = self._args.compress, mode = mode) as self._snapshot_writer:
			self._snapshot_writer.create(progress_callback = self._progress, progress_callback_period = self._args.commit_period)
