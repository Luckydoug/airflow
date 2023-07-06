#!/bin/bash

USER=biadmin
PASS=@MAktub/..

LOG=/home/opticabi/airflow/dags/sub_tasks/ticketing_data/clearCache.log

logCacheClear() {
    if [ "$1" = "START" ]; then
        echo "[`date +"%Y/%m/%d %H:%M:%S"`] ========== Cleaning $2 cache ==========" >> $LOG
    else
        echo >> $LOG
        echo "[`date +"%Y/%m/%d %H:%M:%S"`] ========== Finished cleaning $2 cache ==========" >> $LOG
    fi
}

execCacheClear() {
    echo -n "Cleaning $1 cache..."
    logCacheClear START $1
    wget --quiet --auth-no-challenge --http-user=$USER --http-password=$PASS -O - "$2" >> $LOG
    logCacheClear STOP $1
    echo " ok"
}


execCacheClear "Dashboards" "http://localhost:8080/pentaho/plugin/cda/api/clearCache"
