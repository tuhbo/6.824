#!/bin/bash

for i in $(seq 1 100)
do
    go test -run 3A
done