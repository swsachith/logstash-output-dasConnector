#!/usr/bin/env bash
gem build logstash-output-dasConnector.gemspec

sh $LOGSTASH_HOME/bin/plugin install logstash-output-dasConnector-1.0.0.gem
