#!/bin/bash

check_home=`cd ./CheckGraphColor; pwd`
echo "set up CheckGraphColor.so in $check_home"


#1. check linux version

res=`cat /etc/os-release | grep 'Ubuntu 16.04.6 LTS'`
if [ "$res" = "" ]; then
  echo "[Error] Please choose provided virtual machine or docker. "
  exit
fi

#2. check os bit

vm_so=$check_home/CheckGraphColor.so.vm
docker_so=$check_home/CheckGraphColor.so.docker
target=$check_home/CheckGraphColor.so

if [ -e $target ]; then
  echo "rm $target and recreate it"
  rm $target
fi

res=`uname -m`
if [ "$res" = "i686" ]; then
  echo "set up $target for 32-bit virtual machine"
  ln -s $vm_so $target
elif [ "$res" = "x86_64" ]; then
  echo "set up $target for 64-bit docker"
  ln -s $docker_so $target
else
  echo "[Error] Please choose provided virtual machine or docker. "
  exit
fi
