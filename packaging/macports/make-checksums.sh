#!/bin/bash
# NB: this script requires bash
tarball_src=$1
tarball_bin=$2
for type in src bin
do
    tarball_var=tarball_${type}
    tarball=${!tarball_var}
    for algo in sha1 rmd160
    do
        checksum=$(openssl $algo ${tarball} | awk '{print $NF}')
        echo "s|@$algo-$type@|$checksum|g"
    done
done
