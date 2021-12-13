# Setup
Run the following commands in different terminals
```shell
fulva@fulvahost:~/iotxs$ socat /dev/serial/by-id/... TCP-LISTEN:9995
```
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.lock.operation_app
```
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.lock.io_bridge
```
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.device.app
```

# Usage
For a client with its name "{ClientName}"

Publish lock requests to topic
```
iotxs/{ClientName}/lock
```
Subscribe to
```
iotxs/{ClientName}/lock/notification
```
for responses and notifications

Publish device requests for a device named "{DeviceName}" to topic
```
iotxs/{ClientName}/device/{DeviceName}
```
Subscribe to
```
iotxs/{ClientName}/device/{DeviceName}/res
```
for responses
