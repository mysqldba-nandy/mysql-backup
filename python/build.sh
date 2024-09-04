#!/bin/bash

# enter
cd $(dirname $0)

# build
pyinstaller --clean --onefile main.py --name mysql_backup
mv dist/mysql_backup .

# clean
rmdir dist
rm -rf build
rm -f mysql_backup.spec
