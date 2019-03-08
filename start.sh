#!/usr/bin/env bash
# This is mostly not working right now. Stop with
# kill $(ps aux | grep dask- | awk '{print $2}')
# though be careful...
set -euo pipefail

# OPTS=`getopt -o hn: --long scheduler,nthreads: 'parse-options' -- "$@"`
# if [ $? != 0  ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi

# eval set -- "$OPTS"

HOST=10.33.225.160
NTHREADS=10

PIDS=()


# while true; do
#     echo $1
#   case "$1" in
#     -h | --host )     HOST=$2; shift; shift ;;
#     # -h | --help )    HELP=true; shift ;;
#     -n | --nthreads ) NTHREADS="$2"; shift; shift ;;
#     -- ) shift; break ;;
#     * ) break ;;
#   esac
# done

# echo $HOST
# echo $NTHREADS

function handle_sigint()
{
    for proc in `jobs -p`
    do
        kill $proc
    done
}


dask-scheduler \
    --host=ucx://${HOST}:13337 \
    --pid-file=scheduler.pid \
    --scheduler-file=scheduler.json&
PIDS+=$!

sleep 2
echo "PIDS: ${PIDS}"

for i in `seq 1 10`; do
    echo "starting worker ${i}"
    dask-worker --scheduler-file=scheduler.json \
        --host=${HOST} \
        --nthreads=${NTHREADS}&
    PIDS+=$!
done

trap handle_sigint SIGINT

echo ${PIDS}

for pid in ${pids}; do
    wait $pid
done



# # kill $(ps aux | grep dask- | awk '{print $2}')
