#! /bin/bash
# This script just starts the CryptDB proxy on the loopback connection
$EDBDIR/bins/proxy-bin/bin/mysql-proxy --plugins=proxy --event-threads=4 \
--max-open-files=1024 \
--proxy-lua-script=$EDBDIR/mysqlproxy/wrapper.lua \
--proxy-address=127.0.0.1:3307 \
--proxy-backend-addresses=localhost:3306
