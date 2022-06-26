#!/bin/bash

sudo su postgres <<EOF
createdb  test_db;
psql -c "CREATE USER test_user WITH PASSWORD 'password'"
psql -c "grant all privileges on database test_db to test_user"

EOF