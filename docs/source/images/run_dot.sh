#!/usr/bin/env bash

set -o errexit

for in_fname in *.dot
do
  out_fname=${in_fname%.dot}.svg
  dot -Tsvg $in_fname > $out_fname
done
