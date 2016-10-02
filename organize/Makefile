# Copyright 2016 Adam H. Leventhal. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

organize: organize.go
	go build 

docker: organize_docker ca-certificates.crt
	docker build -t organize .

organize_docker: organize.go
	CGO_ENABLED=0 GOOS=linux go build -o organize_docker

ca-certificates.crt: ../common/ca-certificates.crt
	cp ../common/ca-certificates.crt .

clean:
	rm -f organize organize_docker ca-certificates.crt
