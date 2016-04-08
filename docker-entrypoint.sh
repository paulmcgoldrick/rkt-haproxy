#!/bin/sh
set -e


# Set environment variables HA_ETH#_IP=<ip address>
for interface in `ip addr | grep 'inet ' | grep eth | awk '{ print toupper($5)}' `; 
do 
        export eval "HA_${interface}_IP"=$(ip addr show `echo $interface | tr '[:upper:]' '[:lower:]'` | grep 'inet ' | awk '{ print $2}' | sed -e 's/\/.*//g')
done

if [ "${1#-}" != "$1" ]; then
    set -- haproxy "$@"
fi

if [ "$1" = 'haproxy' ]; then
    shift # "haproxy"
    set -- "$(which haproxy-systemd-wrapper)" -p /run/haproxy.pid "$@"
fi

exec "$@"
