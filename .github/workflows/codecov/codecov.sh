#!/bin/bash
set -e

apt-get update && \
apt-get install -y \
curl \
libcurl4-openssl-dev \
libelf-dev \
libdw-dev \
cmake \
gcc \
binutils-dev \
libiberty-dev

wget https://github.com/SimonKagstrom/kcov/archive/master.tar.gz &&
tar xzf master.tar.gz &&
cd kcov-master &&
mkdir build &&
cd build &&
cmake .. &&
make &&
make install DESTDIR=../../kcov-build &&
cd ../.. &&
ls &&
rm -rf kcov-master &&
for file in target/debug/incremental/pravega_client_rust-*; do [ -x "${file}" ] || continue; mkdir -p "target/cov/$(basename $file)"; ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"; done &&
bash <(curl -s https://codecov.io/bash) -t 6af47fa4-eed7-4fce-adbd-77291bc74cac &&
echo "Uploaded code coverage"