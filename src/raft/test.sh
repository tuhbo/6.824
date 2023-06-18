#!/bin/bash

for i in $(seq 1 20)
do
  go test -run 2A
done
