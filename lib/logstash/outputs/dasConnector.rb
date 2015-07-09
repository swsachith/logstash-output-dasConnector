# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"

class LogStash::Outputs::DASConnector < LogStash::Outputs::Base
  # This output lets you `PUT` or `POST` events to a
  # generic HTTP(S) endpoint
  #
  # Additionally, you are given the option to customize
  # the headers sent as well as basic customization of the
  # event json itself.

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

  config :arbitraryValues, :required => :true, :validate => :hash

  config :payloadFields, :required => :true, :validate => :hash

  config :metaData, :required => :true

  config :correlationData, :required => :true

  config :streamDefinition, :required => :true, :validate => :hash

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
    if @format == "message"
      if @eventData.nil?
        raise "eventData must be set if message format is used"
      end
      if @content_type.nil?
        raise "content_type must be set if message format is used"
      end
      unless @mapping.nil?
        @logger.warn "mapping is not supported and will be ignored if message format is used"
      end
    end
  end

  # def register

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

    # create the Log event
    wso2EventResponse = createWSO2Event(modifiedEvent, event)

    #adding stream definition
    addStreamRequest = addStreamDefinition(@agent, modifiedEvent, event)


    puts "adding stream definition \n"
    #puts event.sprintf(@url)
    puts addStreamRequest

    puts "printing wso2 event ==== \n"
    puts wso2EventResponse
  end

  # def receive

  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key) + "=" + CGI.escape(value)
    end.join("&")
  end

  # def encode


  # ---- WSO2 connector related event configuration ------
  # --------example configuration ------------
  #          streamId : "TEST:1.0.0",
  #          timestamp : 54326543254532, "optional"
  #          payloadData : {
  #          },
  #          metaData : {
  #          },
  #          correlationData : {
  #          }
  #          arbitraryDataMap : {
  #          }
  def createWSO2Event(modifiedEvent, event)


    case @http_method
      when "put"
        request = @agent.put(event.sprintf(@url))
      when "post"
        request = @agent.post(event.sprintf(@url))
      else
        @logger.error("Unknown verb:", :verb => @http_method)
    end

    if @headers
      @headers.each do |k, v|
        request.headers[k] = event.sprintf(v)
      end
    end

    request["Content-Type"] = @content_type


    #constructing the wso2Event
    wso2Event = Hash.new

    #processing the payloadData Field
    @payloadData = Hash[@payloadFields.map { |key, value| [key, modifiedEvent[key]] }]

    # getting the arbitrary values map with its values from the event
    @processedArbitraryValues = Hash[@arbitraryValues.map { |key, value| [key, modifiedEvent[key]] }]

    puts @processedArbitraryValues

    wso2Event["streamId"] = modifiedEvent["streamId"]
    wso2Event["payloadData"] = @payloadData
    wso2Event["metaData"] = @metaData
    wso2Event["correlationData"] = @correlationData
    wso2Event["arbitraryDataMap"] = @processedArbitraryValues

    begin
      if @format == "json"
        request.body = LogStash::Json.dump(wso2Event)
      elsif @format == "message"
        request.body = event.sprintf(@eventData)
      else
        request.body = encode(evt)
      end
      #puts "#{request.port} / #{request.protocol}"
      #puts request
      #puts
      #puts request.body
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

  #example stream definition
  # streamDef = {
  #          name : "TEST",
  #          version : "1.0.0",
  #          nickName : "test",
  #          description : "sample description"
  #          payloadData : {
  #              name : "STRING",
  #              married : "BOOLEAN",
  #              age : "INTEGER"
  #          },
  #         metaData : {
  #              timestamp : "LONG"
  #          },
  #         correlationData : {
  #
  #          },
  #          tags : []
  #      }
  public
  def addStreamDefinition(agent, processedEvent, recievedEvent)
    streamDefinition = Hash.new
    streamDefinition = @streamDefinition
    streamDefinition["payloadData"] = @payloadFields
    case @http_method
      when "put"
        addStreamRequest = agent.put(recievedEvent.sprintf(@url))
      when "post"
        addStreamRequest = agent.post(recievedEvent.sprintf(@url))
      else
        @logger.error("Unknown verb:", :verb => @http_method)
    end
    if @headers
      @headers.each do |k, v|
        addStreamRequest.headers[k] = recievedEvent.sprintf(v)
      end
    end

    addStreamRequest["Content-Type"] = @content_type

    if @format == "json"
      addStreamRequest.body = LogStash::Json.dump(streamDefinition)
    end

    addStream_response = agent.execute(addStreamRequest)

    # Consume body to let this connection be reused
    #rbody = ""
    #response.read_body { |c| rbody << c }

    return addStreamRequest.body
  end

  def getSc(agent, processedEvent, recievedEvent)
    streamDefinition = Hash.new
    streamDefinition = @streamDefinition
    streamDefinition["payloadData"] = @payloadFields
    case @http_method
      when "put"
        addStreamRequest = agent.put(recievedEvent.sprintf(@url))
      when "post"
        addStreamRequest = agent.post(recievedEvent.sprintf(@url))
      else
        @logger.error("Unknown verb:", :verb => @http_method)
    end
    if @headers
      @headers.each do |k, v|
        addStreamRequest.headers[k] = recievedEvent.sprintf(v)
      end
    end

    addStreamRequest["Content-Type"] = @content_type

    if @format == "json"
      addStreamRequest.body = LogStash::Json.dump(streamDefinition)
    end

    addStream_response = agent.execute(addStreamRequest)

    # Consume body to let this connection be reused
    #rbody = ""
    #response.read_body { |c| rbody << c }

    return addStreamRequest.body
  end

  def getStreamDefinition(agent)

  end

  def setStreamDefinition(agent)

  end
end