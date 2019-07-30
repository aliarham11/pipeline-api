#!/usr/bin/env bash

function start () {
    if [ -z $1 ]; then host=0.0.0.0; else host=$1; fi
    if [ -z $2 ]; then port=5000; else port=$2; fi

    gunicorn -b $host:$port start:app
}

function stop () {
    ps aux | grep gunicorn | awk '{print $2}' | xargs kill -9
}

function unittest () {
    # python -m unittest discover convertnd
    python -m pytest pipeline_service/tests
}

case "$1" in
    start)
        start $2 $3 $4
        ;;
    stop)
        stop
        ;;
    test)
        unittest
        ;;
    *)
    echo $"Usage: $0 {start|stop|test} [host] [post]"
    exit 1
esac
