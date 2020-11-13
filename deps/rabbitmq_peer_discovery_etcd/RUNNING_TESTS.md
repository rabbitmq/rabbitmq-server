## Running Common Test Suites

```shell
gmake tests
```        

When running tests via `gmake tests`, there is no need to run the
`init-etcd.sh` script as the test suite will do it for you.


## etcd Node Management

The test suite of this plugin ships with a [script](./test/system_SUITE_data/init-etcd.sh)
that starts an `etcd` node in the background.

This script can also be used to start a node for experimenting with this
plugin. To run it:

```shell
./test/system_SUITE_data/init-etcd.sh [etcd data dir] [etcd client port]
```  

where `[etcd data dir]` is the desired `etcd` node data directory path.

The script depends on the [`daemonize`
tool](https://software.clapper.org/daemonize/) for running the process in the
background and pid file creation.
                                                                   
To stop a node started this way use the pid file created by `daemonize`:

```shell
pkill -INT $(cat [etcd data dir]/etcd.pid)
```              
