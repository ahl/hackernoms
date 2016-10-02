# Copyright 2016 Adam H. Leventhal. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

FROM scratch
ADD ca-certificates.crt /etc/ssl/certs/
ADD organize_docker /organize
ENV GOMAXPROCS 8
ENV TMPDIR /
ENV PATH /
ENTRYPOINT ["organize"]
