#!/bin/bash

i = 0

while ((i < 400))
do
    go test >> log1.txt &
    go test >> log2.txt &
    go test >> log3.txt &
    go test >> log4.txt &
    go test >> log5.txt
    ((i++))
done

