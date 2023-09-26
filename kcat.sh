#!/bin/sh
set -eu

_field () {
  FMT="$FMT""$1"$'\n'
}

FMT=''
_field %t # topic
_field %p # partition
_field %o # offset
_field %T # timestamp
_field %K # key length 
_field %k # key
_field %S # message length 
_field %s # message

exec kcat -e -f "$FMT" -C -b "$BROKER" -t "$TOPIC"

