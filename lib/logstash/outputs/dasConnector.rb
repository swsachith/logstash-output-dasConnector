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

  config :payloadData, :required => :true, :validate => :array

  config :arbitraryValues, :required => :true, :validate => :hash

  config :metaData, :required => :true

  config :correlationData, :required => :true

  public
  def register
    require "ftw"
    require "uri"
    @agent = FTW::Agent.new
    # TODO(sissel): SSL verify mode?

    if @content_type.nil?
      case @format
        when "form" ; @content_type = "application/x-www-form-urlencoded"
        when "json" ; @content_type = "application/json"
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
  end # def register

  public
  def receive(event)
    return unless output?(event)

    if @mapping
      evt = Hash.new
      @mapping.each do |k,v|
        evt[k] = event.sprintf(v)
      end
    else
      evt = event.to_hash
    end

    wso2Event = createWSO2Event(evt)
    puts evt
    puts "printing wso2 event ==== \n"
    puts wso2Event

    case @http_method
      when "put"
        request = @agent.put(event.sprintf(@url))
      when "post"
        request = @agent.post(event.sprintf(@url))
      else
        @logger.error("Unknown verb:", :verb => @http_method)
    end

    if @headers
      @headers.each do |k,v|
        request.headers[k] = event.sprintf(v)
      end
    end

    request["Content-Type"] = @content_type

    begin
      if @format == "json"
        request.body = LogStash::Json.dump(wso2Event)
      elsif @format == "message"
        request.body = event.sprintf(@eventData)
      else
        request.body = encode(evt)
      end
      #puts "#{request.port} / #{request.protocol}"
      puts request
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
  end # def receive

  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key) + "=" + CGI.escape(value)
    end.join("&")
  end # def encode

    def createWSO2Event(event)

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

      #constructing the wso2Event
      wso2Event = Hash.new

      #processing the payloadData Field
      @payloadData.map! { |item| item = event[item] }

      # getting the arbitrary values map with its values from the event
      @processedArbitraryValues = Hash[@arbitraryValues.map{|key,value| [key,event[key]] } ]

      puts @processedArbitraryValues

      wso2Event["streamId"] = event["streamId"]
      wso2Event["payloadData"] = @payloadData
      wso2Event["metaData"] = @metaData
      wso2Event["correlationData"] = @correlationData
      wso2Event["arbitraryDataMap"] = @processedArbitraryValues

      return wso2Event
    end
end