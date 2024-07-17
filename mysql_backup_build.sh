#!/bin/bash

# enter
cd $(dirname $0)

# build
pyinstaller --clean --onefile mysql_backup.py
mv dist/mysql_backup .

# clean
rmdir dist
rm -rf build
rm -f mysql_backup.spec
