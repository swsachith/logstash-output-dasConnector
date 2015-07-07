#!/usr/bin/env bash
cd ..
rm -rf logstash-1.5.1
tar xf logstash-1.5.1.tar.gz
cd logstash-output-dasConnector
sh installGem.sh
sh run.sh