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

import subprocess
import json
import uuid
import tempfile
import os
import collections

class Certificates():
	_CertificateKey = collections.namedtuple("CertificateKey", [ "cert", "key", "trusted_peer_certs" ])

	@classmethod
	def create_cert_key(cls, output_filename):
		cert_uuid = str(uuid.uuid4())
		private_key = subprocess.check_output([ "openssl", "ecparam", "-name", "secp384r1", "-genkey" ])
		with tempfile.NamedTemporaryFile(mode = "wb") as keyfile:
			keyfile.write(private_key)
			keyfile.flush()
			certificate = subprocess.check_output([ "openssl", "req", "-x509", "-subj", "/CN=%s" % (cert_uuid), "-days", str(365 * 100), "-key", keyfile.name ])
		with open(os.open(output_filename, os.O_CREAT | os.O_WRONLY, 0o600), "w") as f:
			cert_key = {
				"cert": certificate.decode("ascii"),
				"key": private_key.decode("ascii"),
				"trusted_peer_certs": [ ],
			}
			json.dump(cert_key, f)
		return cls._CertificateKey(cert = cert_key["cert"], key = cert_key["key"], trusted_peer_certs = cert_key["trusted_peer_certs"])

	@classmethod
	def add_cert_key_trusted_peers(cls, keyfile_filename, trusted_peers):
		with open(keyfile_filename) as f:
			cert_key = json.load(f)
		cert_key["trusted_peer_certs"] += trusted_peers
		with open(os.open(keyfile_filename, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
			json.dump(cert_key, f)

	@classmethod
	def load_cert_key(cls, filename):
		with open(filename) as f:
			cert_key = json.load(f)
		return cls._CertificateKey(cert = cert_key["cert"], key = cert_key["key"], trusted_peer_certs = cert_key["trusted_peer_certs"])

	@classmethod
	def create_server_client_keys(cls, server_filename, client_filename):
		server = Certificates.create_cert_key(server_filename)
		client = Certificates.create_cert_key(client_filename)
		cls.add_cert_key_trusted_peers(server_filename, [ client.cert ])
		cls.add_cert_key_trusted_peers(client_filename, [ server.cert ])

if __name__ == "__main__":
	Certificates.create_server_client_keys("server.json", "client.json")
