# Setup
Run the following commands in different terminals
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.lock.operation_app
```
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.lock.io_bridge
```
```shell
fulva@fulvahost:~/iotxs$ python -m iotxs.device.app
```
```shell
fulva@fulvahost:~/iotxs$ socat /dev/serial/by-id/... TCP-LISTEN:9995
```
