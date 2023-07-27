#!/bin/bash
set -x
export DB_HOST=127.0.0.1
export DB_DRIVER=postgresql
export DB_PORT=5432
export DB_USER=postgres
export DB_PW=Bmlc_2021
export DB_DATABASE=paddleflow
export PATH=/usr/pgsql-10/bin:/usr/bin;
if [[ $DB_DRIVER = "postgresql" ]];then
  psql -U $DB_USER -h$DB_HOST -p$DB_PORT --c "\c $DB_DATABASE"  #&>/dev/null
  if [ $? -ne 0 ]
  then
   echo "MySQL database $DB_DATABASE is not exist"
     psql -U $DB_USER -h$DB_HOST -p$DB_PORT --c "CREATE DATABASE $DB_DATABASE"
    echo 'Create Database...'
  else
   echo "PostgreSQL database $DB_DATABASE is exist, starting backup."
   pg_dump -U $DB_USER -h$DB_HOST -p$DB_PORT -d $DB_DATABASE -f /home/postgres/`date +"%Y-%m-%d-%H-%M-%S"`-postgres.sql
   psql -U $DB_USER -h$DB_HOST -p$DB_PORT --c "DROP DATABASE $DB_DATABASE"
   psql -U $DB_USER -h$DB_HOST -p$DB_PORT --c "CREATE DATABASE $DB_DATABASE"
  fi
  cp /job/paddleflow.sql /job/paddleflow.sql.bak
  sed -i "s/paddleflow_db/$DB_DATABASE/g" /job/paddleflow.sql.bak
  echo "creating database $DB_DATABASE."
  psql -d $DB_DATABASE -U $DB_USER -h$DB_HOST -p$DB_PORT -f /job/paddleflow.sql.bak
  rm -rf /job/paddleflow.sql.bak
  echo "test database $DB_DATABASE."
elif [[ $DB_DRIVER = "postgres" ]];then
  echo "not implement"
fi
