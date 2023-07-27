#!/bin/bash
set -x
export DB_HOST="主机名"
export DB_DRIVER=postgresql #数据库类型
export DB_PORT= #端口
export DB_USER=postgres #用户
export DB_PW= #密码
export DB_DATABASE=paddleflow #数据库名称
export PATH=/usr/pgsql-10/bin:/usr/bin; #二进制文件地址
if [ $DB_DRIVER == "mysql" ];then
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "use $DB_DATABASE" &>/dev/null
  if [ $? -ne 0 ]
  then
   echo "MySQL database $DB_DATABASE is not exist"
  else
   echo "MySQL database $DB_DATABASE is exist, starting backup."
   mysqldump -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT --databases $DB_DATABASE >  $DB_DATABASE.bak_`date +%Y%m%d`.sql
   mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "drop database if exists $DB_DATABASE;"
  fi
  echo "creating database $DB_DATABASE."
  sed -i "s/paddleflow_db/$DB_DATABASE/g" paddleflow.sql
  mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT -e "source paddleflow.sql"
  echo "creating database $DB_DATABASE completed."
elif [[ $DB_DRIVER = "postgres" ]];then
  psql -U $DB_USER -h$DB_HOST -p$DB_PORT --c "\c $DB_DATABASE"  #&>/dev/null
  if [ $? -ne 0 ]
  then
   echo "PostgreSQL database $DB_DATABASE is not exist"
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
else
  echo 'Database tool filling error, please check!'
fi
