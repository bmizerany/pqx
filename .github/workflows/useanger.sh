#!/bin/sh
cue export --out yaml ci.cue > ci.yml
git add .
git commit -m 'testing CI'
git push

set -e

sleep 1
gh run watch
