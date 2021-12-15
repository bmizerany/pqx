set -e
set -x

export PATH="$PATH:/usr/lib/postgresql/13/bin/"
export PATH="$PATH:/usr/local/go/bin"

cd /github/workspace

go test -v ./...
