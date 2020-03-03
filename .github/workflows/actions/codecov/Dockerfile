FROM kcov/kcov:latest

RUN apt-get update && \
    apt-get install -y \
    curl \
    bash

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]