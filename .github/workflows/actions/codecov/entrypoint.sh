#!/bin/sh

set -e

for file in target/debug/examplerust-*
do
  [ -x "${file}" ] || continue
  mkdir -p "target/cov/$(basename $file)"
  /usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"
done

bash <(curl -s https://codecov.io/bash)

echo "Uploaded code coverage"