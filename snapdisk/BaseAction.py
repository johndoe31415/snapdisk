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

class BaseAction():
	def __init__(self, cmdname, args):
		self._cmdname = cmdname
		self._args = args
		self.run()

	def run(self):
		raise NotImplementedError(self.__class__.__name__)
