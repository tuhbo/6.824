#!/bin/bash

for i in $(seq 100)
do
    go test -run Test
done