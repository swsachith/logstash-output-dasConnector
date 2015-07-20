=begin
/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
=end

# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"

#Util modules of the DAS Connector
require "stream_manager"
require "schema_manager"

require "json"
require "date"

class LogStash::Outputs::DASConnector < LogStash::Outputs::Base

  #name of the plugin
  config_name "dasConnector"

  # URL to use
  config :url, :validate => :string, :required => :true

  # validate SSL?
  config :verify_ssl, :validate => :boolean, :default => true

  # What verb to use
  # only put and post are supported for now
  config :http_method, :validate => ["put", "post"], :required => :true

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash

  # Content type
  #
  # If not specified, this defaults to the following:
  #
  # * if format is "json", "application/json"
  # * if format is "form", "application/x-www-form-urlencoded"
  config :content_type, :validate => :string

  # This lets you choose the structure and parts of the event that are sent.
  #
  #
  # For example:
  # [source,ruby]
  #    mapping => ["foo", "%{host}", "bar", "%{type}"]
  config :mapping, :validate => :hash

  # validate SSL?
  config :verify_ssl, :validate => :boolean, :default => true

  # Set the format of the http body.
  #
  # If form, then the body will be the mapping (or whole event) converted
  # into a query parameter string, e.g. `foo=bar&baz=fizz...`
  #
  # If message, then the body will be the result of formatting the event according to message
  #
  # Otherwise, the event is sent as json.
  config :format, :validate => ["json", "form", "message"], :default => "json"

  config :eventData, :validate => :string

  #
  # ------- WSO2 Custom event related configs ------------
  #

  # The ID of the corresponding stream it publishes to
  # This must be configured in the logstash configuration file
  #

  #event related data
  config :payloadFields, :required => :true, :validate => :hash
  config :metaData, :required => :true
  config :correlationData, :required => :true
  config :arbitraryValues, :required => :true, :validate => :hash

  #schema related details
  config :schemaDefinition, :required => :true, :validate => :hash
  config :streamName, :required => :true, :validate => :string
  config :streamVersion, :required => :true, :validate => :string

  public
  def register
    require "ftw"
    require "uri"
    @agent = FTW::Agent.new
    # TODO(sissel): SSL verify mode?

    if @content_type.nil?
      case @format
        when "form";
          @content_type = "application/x-www-form-urlencoded"
        when "json";
          @content_type = "application/json"
      end
    end

    processedURL = @url  + "/portal/controllers/apis/analytics.jag"

    #Get the Schema
    current_schema = SchemaManager.getSchemaDefinition(@agent,processedURL, @schemaDefinition)
    puts "******************printing the current schema ****************************************\n"
    puts current_schema
    puts "************************setting the new schema *******************\n"
    #setting the new schema if required
    puts SchemaManager.setSchemaDefinition(@agent, @payloadFields, @arbitraryValues, @correlationData,@metaData,@schemaDefinition,current_schema,processedURL)

  end

  public
  def receive(event)
    return unless output?(event)

    #setting the base parameters for the http request
    if @mapping
      modifiedEvent = Hash.new
      @mapping.each do |k, v|
        modifiedEvent[k] = event.sprintf(v)
      end
    else
      modifiedEvent = event.to_hash
    end

    # send the log event
    wso2EventResponse = sendWSO2Event(modifiedEvent, event)

  end

  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key) + "=" + CGI.escape(value)
    end.join("&")
  end

  #-- This method creates the WSO2 Events from the logstash events and sends them
  def sendWSO2Event(modifiedEvent, event)
    publishURL =  @url+"/portal/controllers/apis/analytics.jag?type=24"

    case @http_method
      when "put"
        request = @agent.put(event.sprintf(publishURL))
      when "post"
        request = @agent.post(event.sprintf(publishURL))
      else
        @logger.error("Unknown verb:", :verb => @http_method)
    end

    if @headers
      @headers.each do |k, v|
        request.headers[k] = event.sprintf(v)
      end
    end

    request.headers["Authorization"] = "Basic YWRtaW46YWRtaW4="

    request["Content-Type"] = "application/json"

    #constructing the wso2Event
    wso2Event = Hash.new

    #processing the payloadData Field
    @payloadData = Hash[@payloadFields.map { |key, value| [key, modifiedEvent[key]] }]

    #processing the payloadData Field
    @metaDataMap = Hash[@metaData.map { |key, value| [key, modifiedEvent[key]] }]

    #processing the correlationData Field
    activityID = modifiedEvent["activity_id"]
    if activityID.nil?
      activityID = (0...8).map { (65 + rand(26)).chr }.join
    end

    activityArray = Array.new(1)
    activityArray[0] = activityID
    @correlationData["activity_id"] = activityArray

    # getting the arbitrary values map with its values from the event
    @processedArbitraryValues = Hash[@arbitraryValues.map { |key, value| [key, modifiedEvent[key]] }]


    #puts @processedArbitraryValues
    #@arbitraryValues["timestamp"] = DateTime.parse(modifiedEvent["syslog_timestamp"]).to_time.to_i
    #puts modifiedEvent["syslog_timestamp"]
    wso2Event["streamName"] = @streamName
    wso2Event["streamVersion"] = @streamVersion
    wso2Event["payloadData"] = @payloadData
    wso2Event["metaData"] = @metaDataMap
    wso2Event["correlationData"] = @correlationData
    wso2Event["arbitraryDataMap"] = @processedArbitraryValues

    begin
      request.body = LogStash::Json.dump(wso2Event)
      response = @agent.execute(request)

      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
        #puts rbody
    rescue Exception => e
      @logger.warn("Unhandled exception", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end
    return response
  end

end