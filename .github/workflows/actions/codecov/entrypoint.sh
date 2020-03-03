#!/bin/sh

set -e

DESTDIR="../../kcov-build"
BUILD="build"
KCOVMASTER="kcov-master"

echo "Getting kcov files"
sh -c "wget -nv https://github.com/SimonKagstrom/kcov/archive/master.tar.gz"
echo "Extracting kcov content"
tar -xzf master.tar.gz

ls -l

echo "Moving into the extracted directory"
cd $KCOVMASTER

echo "Creating the build folder"
mkdir $BUILD

echo "Moving into the build folder"
cd $BUILD

cmake ..

make

make install $DESTDIR

cd ../..

rm -rf $KCOVMASTER

for file in target/debug/examplerust-*
do
  [ -x "${file}" ] || continue
  mkdir -p "target/cov/$(basename $file)"
  ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"
done

bash <(curl -s https://codecov.io/bash)

echo "Uploaded code coverage"