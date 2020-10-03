# snapdisk
snapdisk is a user-mode utility that allows creating snapshots of block-devices
in pieced chunks with deduplication. Effectively, it can be used to create
backups of block devices. It is currently under development and may not have
all functionality implemented fully.

## Usage
snapdisk has multiple facilities, as seen in the help page:

```
Syntax: ./snapdisk.py [command] [options]

Available commands:
    snapshot           Create a snapshot of a block device
    serve              Start a snapshot server that serves an image
    genkey             Generates a server and client key for use with TLS

Options vary from command to command. To receive further info, type
    ./snapdisk.py [command] --help
```

The easiest is to create a local device block snapshot:

```
$ ./snapdisk.py snapshot /dev/sda1 backup-image
```

But when snapdisk is also installed at a peer, it can also be tunneled over
ssh:

```
$ ./snapdisk.py snapshot ssh://root@myserver.com//dev/sda1 backup-image
```

Alternatively, you can start a server on the peer, e.g., over plain IP:

```
$ ./snapdisk.py serve -e ip://192.168.1.100 /dev/sdb5
```

And then, on the client side, create the backup by connecting to it:

```
$ ./snapdisk.py snapshot ip://192.168.1.100 backup-image-remote
```

Lastly, you can create TLS keypairs for client and server side:

```
$ ./snapdisk.py genkey server.json client.json
```

And then use the TLS server/client functionality. On the server using the
server key:

```
$ ./snapdisk.py serve tls://192.168.1.100/server.json /dev/sdb9
```

And on the client using the client key:

```
$ ./snapdisk.py snapshot tls://192.168.1.100/client.json backup-image-tls
```

All individual commands have their own help pages and offer many options,
consult them to learn more.

## License
GNU GPL-3.
