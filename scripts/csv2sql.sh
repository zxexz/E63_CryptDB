#! /bin/bash
# Pipe a 6 column CSV file, comma delimited, through this script - it will output a SQL script (for table USERDATA)
awk -F',' '{ printf "INSERT INTO USERDATA VALUES (\x27%s\x27,\x27%s\x27,\x27%s\x27,\x27%s\x27,\x27%s\x27,\x27%s\x27);",$1,$2,$3,$4,$5,$6;print ""}'
