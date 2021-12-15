FROM golang:1.17.5-bullseye

# Install postgres and friends
RUN apt-get update
RUN apt-get -y install postgresql

# See: https://github.com/docker-library/postgres/issues/415
RUN localedef -i en_US -f UTF-8 en_US.UTF-8

# In the future, it may be best to create a ci user and not use postgres, but
# that isn't neccesarry ATM, so punting.
USER postgres

ENV PATH="$PATH:/usr/lib/postgresql/13/bin/"
ENV PATH="$PATH:/usr/local/go/bin"
ENV GOCACHE=/tmp/.cache

CMD ["go", "test", "-v", "./..."]
