# Demo add with IBM MQ
This is a simple demo of how to use the `ibm-mq` package to add a message to an IBM MQ queue.

It uses the `libmqm-sys` and `mqi` crates to connect to the queue manager and put a message on the queue.

## Pre-requisites

Install IBM MQ redistributable client on your system. You can download it from the [IBM MQ download page](https://www.ibm.com/support/fixcentral/swg/selectFixes?parent=ibm~WebSphere&product=ibm/WebSphere/WebSphere+MQ&release=9.4.0.0&platform=All&function=fixid&fixids=*IBM-MQC-Redist-*).

Choose the Linux x64 version of the redistributable client. After downloading, extract the files to a directory of your choice.

Make sure to set the `MQ_HOME` environment variable to the directory where you extracted the files. For example, if you extracted the files to `/opt/mqm`, you can set the environment variable like this:

```bash
export MQ_HOME=/opt/mqm
```
Also make sure that you have the `LD_LIBRARY_PATH` environment variable set to the `lib64` directory of the redistributable client. For example:

```bash
export LD_LIBRARY_PATH=$MQ_HOME/lib64:$LD_LIBRARY_PATH
```


Install `cargo-workspace-lints`

```bash
cargo install cargo-workspace-lints
```


## Environment Variables

Example of setting the environment variables in Linux:

`MQSERVER`: `DEV.ADMIN.SVRCONN/TCP/localhost(1414)`

## Starting the Queue Manager

To start the queue manager, run the following command:

```bash
docker run -it -e LICENSE=accept --env MQ_QMGR_NAME=QM8 --env MQ_ADMIN_PASSWORD=passw0rd --env MQ_APP_PASSWORD=passw0rd --publish 1414:1414 --publish 9443:9443 icr.io/ibm-messaging/mq:9.4.2.1-r1
```

## Starting the producer

```bash
RUST_LOG=info cargo run -p producer
```

## Starting the consumer

```bash
RUST_LOG=info cargo run -p consumer
```