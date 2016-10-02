# Copyright 2016 Adam H. Leventhal. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

sync: sync.go
	go build 

docker: sync_docker ca-certificates.crt
	docker build -t sync .

sync_docker: sync.go
	CGO_ENABLED=0 GOOS=linux go build -o sync_docker

ca-certificates.crt: ../common/ca-certificates.crt
	cp ../common/ca-certificates.crt .

clean:
	rm -f sync sync_docker ca-certificates.crt
