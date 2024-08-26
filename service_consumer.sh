#! /bin/bash

cd /root/ai-module/mlops-srs

srs_log_dir=/root/ai-log/srs/consumer/proc

if [ ! -d $srs_log_dir ]
then
    mkdir -p $srs_log_dir
fi

date_format=$(date '+%Y-%m-%d_%H:%M:%S')
srs_log=${srs_log_dir}/${date_format}_srs.log

nohup python request_service_pg.py >> $srs_log 2>&1 &
nohup python log_serving.py &

service ntp start

tail -f /dev/null