#!/bin/bash

for i in $(seq 100)
do
    go test -run 2B
done