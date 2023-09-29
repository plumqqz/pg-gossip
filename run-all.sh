#!/bin/sh
./spreader.sh &
./spreader.sh "host=localhost port=45432 user=postgres password=root" &
./spreader.sh "host=localhost port=45433 user=postgres password=root" &
./spreader.sh "host=localhost port=45434 user=postgres password=root" &
