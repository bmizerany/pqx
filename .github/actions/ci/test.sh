set -e
imageID=$(docker build -q .)
dir=$(git rev-parse --show-toplevel)
docker run --rm -it --workdir=/github/workspace -e CI=true -v $dir:/github/workspace "$imageID" "$@"
