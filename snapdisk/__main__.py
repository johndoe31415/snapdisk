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
from .MultiCommand import MultiCommand
from .FriendlyArgumentParser import baseint_unit
from .Endpoints import EndpointDefinition
from .ActionSnapshot import ActionSnapshot
from .ActionServe import ActionServe

mc = MultiCommand()

def genparser(parser):
	parser.add_argument("-p", "--commit-period", metavar = "size", type = baseint_unit, default = "10 Gi", help = "Commit the snapshot file in this interval of time to preserve the progress. Can use an SI or binary suffix, defaults to %(default)s.")
	parser.add_argument("-n", "--name", metavar = "snapshot_name", help = "Snapshot name. If omitted, by default the snapshot is named by the current timestamp.")
	parser.add_argument("-m", "--mode", choices = [ "create", "resume", "overwrite"], default = "create", help = "Snapshotting mode. Can be any of %(choices)s, defaults to %(default)s.")
	parser.add_argument("-c", "--compress", choices = [ "gz" ], default = None, help = "Specify compression method to use for chunks. Can be one of %(default)s, defaults to uncompressed.")
	parser.add_argument("-s", "--chunk-size", metavar = "size", type = baseint_unit, default = "256 Mi", help = "Specify chunk size to use. Can use an SI or binary suffix. Defaults to %(default)s.")
	parser.add_argument("--remote-snapdisk", metavar = "binary", default = "snapdisk.py", help = "When making a snapshot via ssh, this option gives the name of the snapdisk executable on the remote side. Defaults to %(default)s.")
	parser.add_argument("--print-si-units", action = "store_true", help = "By default, units are printed in binary (powers of 1024); this option changes display of all data to SI prefixes (powers of 1000).")
	parser.add_argument("--verbose", action = "count", default = 0, help = "Increase verbosity; can be specified multiple times.")
	parser.add_argument("src", help = "Source image; can be a local block device or a remote URI.")
	parser.add_argument("dst", help = "Destination directory.")
mc.register("snapshot", "Create a snapshot of a block device", genparser, action = ActionSnapshot)

def genparser(parser):
	parser.add_argument("-e", "--endpoint", metavar = "endpoint", type = EndpointDefinition.parse, default = "stdout://", help = "Specify endpoint to use. Can be stdout:// or ip://addr:port or unix://filename. Defaults to %(default)s.")
	parser.add_argument("-m", "--max-chunk-size", metavar = "size", type = baseint_unit, default = "512 Mi", help = "Specify the maximum chunk size that a client may request. Can use an SI or binary suffix. Defaults to %(default)s.")
	parser.add_argument("--verbose", action = "count", default = 0, help = "Increase verbosity; can be specified multiple times.")
	parser.add_argument("src", help = "Source image; must be a local file or block device.")
mc.register("serve", "Start a snapshot server on stdout", genparser, action = ActionServe)

mc.run(sys.argv[1:])
