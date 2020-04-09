FROM node:12-alpine
MAINTAINER info@vizzuality.com

ENV NAME graph-client
ENV USER graph-client

RUN apk update && apk upgrade && \
    apk add --no-cache --update bash git openssh python alpine-sdk

RUN addgroup $USER && adduser -s /bin/bash -D -G $USER $USER

RUN yarn global add grunt-cli bunyan

RUN mkdir -p /opt/$NAME
COPY package.json /opt/$NAME/package.json
RUN cd /opt/$NAME && yarn

COPY entrypoint.sh /opt/$NAME/entrypoint.sh
COPY config /opt/$NAME/config

WORKDIR /opt/$NAME

COPY ./app /opt/$NAME/app
RUN chown -R $USER:$USER /opt/$NAME

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait

# Tell Docker we are going to use this ports
EXPOSE 4500
USER $USER

ENTRYPOINT ["./entrypoint.sh"]
