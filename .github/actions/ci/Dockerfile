FROM golang:1.17.5-bullseye

# Install postgres and friends
RUN apt-get update
RUN apt-get -y install postgresql

ADD init.sh /app/init.sh
ADD ci.sh /app/ci.sh

CMD ["/app/init.sh"]
