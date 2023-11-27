FROM ubuntu:18.04

ENV OPERATOR=/usr/local/bin/query_engine_instance \
    USER_UID=root \
    USER_NAME=discovery
COPY ./cmake/build/query_engnie_instance ${OPERATOR}

ENTRYPOINT ["/usr/local/bin/query_engine_instance"]

USER ${USER_UID}