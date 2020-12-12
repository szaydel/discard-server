# discard-server
This is just a test server, not designed for any production use. Its main purpose is to drop lines of data it receives and count them.


```
# Consume data and print interval stats in one shell
$ go run server.go

# Generate data in another shell
$ ./runtcpclient.sh
```
