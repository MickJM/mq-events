# MQ Events API

## MQ Exporter for Prometheus monitoring

This repository contains Java Spring Boot, microservice code for a monitoring solution that exports queue manager events to a Prometheus data collection system.  It also contains example configuration files on how to run the monitoring program.

The monitor collects events from an IBM MQ v9, v8 or v7 queue manager.  The monitor, read events created by the queue manager Queue Manager events, Channel Events, Configuration events and CPU events and exposes a REST API to return Prometheus output or JSON outout.  Prometheus can be configured to call the exposed end-point at regular intervals to pull these metrics into its database, where they can be queried directly or used with dashboard applications such as Grafana.

The API can be run as a service or from a Docker container.

Queue Manager events are collected, by default, from the SYSTEM.ADMIN.QMGR.EVENT queue.
Channel events are collected, by default, from the SYSTEM.ADMIN.CHANNEL.EVENT queue.
Config events are collected, by default, from the SYSTEM.ADMIN.CONFIG.EVENT queue
CPU events are collected from Publish/Subcribe topic.
* A topic string is created for $SYS/MQ/INFO/QMGR/{Queue Manager}/Monitor
* A SystemSummary queue is created for /CPU/SystemSummary subscription
* A QMGRSummary queue is created for /CPU/QMGRSummary subscription
  
The API can be assigned to used either the QMGR or System statistics by assigning the appropriate queue to 	`ibm.mq.event.cpu.queue`

## Configure IBM MQ

The API can be run in three ways;

* Local binding connection
* Client connection
* Client Channel Definition Table connection

### Local Binding connections

When running with a local binding connection, the API and the queue manager must be running on the same host.  The API connects directly to the queue manager.  No security or authentication is required, as the API is deemed to be authenticated due to it running on the same host.

```
ibm.mq.queueManager: QMGR
ibm.mq.local: true
```

No additional queue manager properties are required, but the APIs common properties can still be used.

### Client Connections

When running as a client connection, the API and the queue manager run on seperate servers, the API connects to the queue manager over a network.  The queue manager must be configured to expose a running MQ listener, have a configured server-connection channel and the appropriate authorities set against the MQ objects (queue manager, queues, channels etc) to issue PCF commands.

Minimum yaml requirements in the application-XXXX.yaml file

```
ibm.mq.queueManager: QMGR
ibm.mq.channel: SVRCONN.CHANNEL.NAME
ibm.mq.connName: HOSTNAME(PORT)
ibm.mq.user: MQUser
ibm.mq.password: secret
ibm.mq.authenticateUsingCSP: true
ibm.mq.local: false
```

`ibm.mq.local` can be true of false, depending if the API connects to queue manager in local binding or client mode.


Connections to the queue manager should be encrypted where possible.  For this, the queue manager needs to be configured with a key-store / trust-store - which can be the same file - and the server-connection channel needs to be configured with a cipher.

```
ibm.mq.useSSL: true
ibm.mq.sslCipherSpec: TLS_RSA_WITH_AES_128_CBC_SHA256
ibm.mq.ibmCipherMapping: false 
ibm.mq.security.truststore: {fully qualified file path}/truststore 
ibm.mq.security.truststore-password: secret
ibm.mq.security.keystore: {fully qualified file path}/keystore 
ibm.mq.security.keystore-password: secret
```
> Keystore and Truststore must be JKS files.

`ibm.mq.useSSL` can be true of false, depending if the MQ server connection channel is configured to be SSL/TLS enabled.

`ibm.mq.ibmCipherMapping` can be true of false, depending on the JVM being used.

### Client Channel Definition Table (CCDT) connections

When running with a CCDT connection, this is similar to a client connection, with the client connection details stored in a secure, binary file.

```
ibm.mq.queueManager: QMGR
ibm.mq.channel: SVRCONN.CHANNEL.NAME   
ibm.mq.ccdtFile: {fully qualified file path}/AMQCLCHL.TAB 
```

All configurations are stored in the Spring Boot yaml or properties file, which it typically located in a `./config` folder under where the API jar file is run from.

## API Endpoints

By default the output of the API is in Prometheus format, but can also be returned in JSON format.

Currently, the API endpoints do not use HTTPS for clients - this will be added in future releases.

### Promtheus Output 

Invoking the API using the below URL, will generate a response in Prometheus format.

`http://{hostname}:{port}/actuator/prometheus`

### JSON Output 

Invoking the API using the below URL, will generate a response in JSON format.

`http://{hostname}:{port}/json/allmetrics` - Returns ALL metrics, including MQ metrics and System metrics generated by the API

`http://{hostname}:{port}/json/mqmetrics` - Returns only MQ metrics - these are all metrics prefixed `mq:`

JSON output can be sorted into Ascending or Descending order using the following properties.

`ibm.mq.json.sort:true|false` - The sort value can be true or false.  True sorts the JSON response into the required order.
`ibm.mq.json.order:ascending|descending` - The order in which to sort the JSON response. By default, the order is in ascending order.


MQ Events API processes events created by a queue manager.

Event messages are generated in [IBMs PCF](https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.1.0/com.ibm.mq.adm.doc/q020000_.htm) format. 

