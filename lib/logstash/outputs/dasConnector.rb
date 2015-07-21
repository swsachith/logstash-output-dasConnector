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
require "base64"

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
  #config :payloadFields
  #config :metaData
  #config :correlationData, :required => :true, :validate => :hash
  config :storedFields, :required => :true, :validate => :hash

  #schema related details
  config :schemaDefinition, :required => :true, :validate => :hash
  config :streamName, :required => :true, :validate => :string
  config :streamVersion, :required => :true, :validate => :string

  # authentication details
  config :username, :required => :true, :validate => :string
  config :password, :required => :true, :validate => :string
  config :authenticationHeader

  # This is the intializer for the plugin
  public
  def register
    require "ftw"
    require "uri"
    @agent = FTW::Agent.new
    # TODO(sissel): SSL verify mode?

    @content_type = "application/json"

    processedURL = @url  + "/portal/controllers/apis/analytics.jag"
    @authenticationHeader = "Basic " + Base64.encode64(@username+":"+@password).strip

    #get the stream definition
    streamDefinition = StreamManager.getStreamDefinition(@agent,processedURL,@authenticationHeader)
    @payloadFields = streamDefinition["payloadData"]
    @metaDataMap = streamDefinition["metaData"]
    @correlationData = streamDefinition["correlationData"]


    #Get the Schema
    current_schema = SchemaManager.getSchemaDefinition(@agent,processedURL, @schemaDefinition,@authenticationHeader)

    #setting the new schema if required
    SchemaManager.setSchemaDefinition(@agent, @payloadFields, @storedFields, @correlationData,@metaData,
                                           @schemaDefinition,current_schema,processedURL,@authenticationHeader)

  end

  # Thie method gets executed for each event recieved
  public
  def receive(event)
    return unless output?(event)

    # send the log event
    wso2EventResponse = sendWSO2Event(event)

  end

  #-- This method creates the WSO2 Events from the logstash events and sends them
  # event => The event received by the plugin
  def sendWSO2Event(event)

    modifiedEvent = event.to_hash
    publishURL =  @url+"/portal/controllers/apis/analytics.jag?type=24"

    #setting the request headers
    request = @agent.post(event.sprintf(publishURL))
    if @headers
      @headers.each do |k, v|
        request.headers[k] = event.sprintf(v)
      end
    end

    request.headers["Authorization"] = @authenticationHeader
    request["Content-Type"] = "application/json"

    #process the timestamp to epoch time
    unless (modifiedEvent["@timestamp"].nil?)
      modifiedEvent["timestamp"] = DateTime.parse(modifiedEvent["@timestamp"].to_s).to_time.to_i
    end

    #processing the payloadData Field
    unless @payloadFields.nil?
      @payloadData = Hash[@payloadFields.map { |key, value| [key, modifiedEvent[key]] }]
    end

    #processing the metadata Field
    unless @metaData.nil?
      @metaDataMap = Hash[@metaData.map { |key, value| [key, modifiedEvent[key]] }]
    end

    #processing the correlationData Field
    # todo : check if there's an correlation id in the stream and do this only if there is
    activityID = modifiedEvent["activity_id"]
    if activityID.nil?
      activityID = (0...8).map { (65 + rand(26)).chr }.join
    end

    activityArray = Array.new(1)
    activityArray[0] = activityID
    @correlationData["activity_id"] = activityArray

    # getting the arbitrary values map with its values from the event
    @processedArbitraryValues = Hash[@storedFields.map { |key, value| [key, modifiedEvent[key]] }]

    #constructing the wso2Event
    wso2Event = Hash.new
    wso2Event["streamName"] = @streamName
    wso2Event["streamVersion"] = @streamVersion
    wso2Event["payloadData"] = @payloadData
    wso2Event["metaData"] = @metaDataMap
    wso2Event["correlationData"] = @correlationData
    wso2Event["arbitraryDataMap"] = @processedArbitraryValues

    begin
      request.body = LogStash::Json.dump(wso2Event)
      response = @agent.execute(request)

      #consume the body
      rbody = ""
      response.read_body { |c| rbody << c }
        #puts rbody
    rescue Exception => e
     # @logger.warn("Excetption Ocurred: ", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end
    puts wso2Event
    return response
  end

end