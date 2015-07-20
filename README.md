############ How to install the plugin ###############

1. Set the LOGSTASH_HOME property
2. Make sure you are using jRuby NOT the normal Ruby
2. a) you can either run the installGem.sh or
2. b) in the terminal run
    gem build logstash-output-dasConnector.gemspec
    sh $LOGSTASH_HOME/bin/plugin install logstash-output-dasConnector-1.0.0.gem

    


bash --login
set the default rvm to use jruby
set the $LOGSTASH_HOME