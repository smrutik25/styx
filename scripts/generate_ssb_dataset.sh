#!/bin/bash

cd "demo/demo-hattrick/HATtrick"
pwd

make all
rm -rf datagen

echo "Creating data directory"
mkdir datagen
./HATtrickBench -gen -pa ./datagen
