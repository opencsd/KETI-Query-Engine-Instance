FROM ubuntu:18.04

ENV OPERATOR=/usr/local/bin/query_engine_instance \
    USER_UID=root \
    USER_NAME=discovery
COPY ./cmake/build/query_engine_instance ${OPERATOR}
COPY ./cmake/snippets /usr/local/snippets
COPY ./shared_library /usr/lib/x86_64-linux-gnu

WORKDIR /usr/local/bin
ENTRYPOINT ["./query_engine_instance"]

USER ${USER_UID}