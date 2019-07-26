#!/bin/bash
export GOPATH=$(pwd)../../../

a=0

until [ ! $a -lt $2 ]
do
  echo $a
  a=`expr $a + 1`

  go test -run $1 > log
  tail ./log -n 10 | grep $3
  if [ $? -ne 0 ]; then
    break;
  fi
done
