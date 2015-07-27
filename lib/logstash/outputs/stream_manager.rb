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

module StreamManager

  # This method gets the stream definition
  # agent => FTW:Agent as the web agent
  # url => URL of the server
  # authenticationHeaders => authenticationInfo
  public
  def StreamManager.getStreamDefinition(agent, url, authenticationHeader,streamName, streamVersion)
    processedSchemaURL = url+"?type=23"

    getStreamDefinition_request = agent.post(processedSchemaURL)
    getStreamDefinition_request.headers["Authorization"] = authenticationHeader
    getStreamDefinition_request["Content-Type"] = "application/json"

    streamInformation = Hash.new
    streamInformation["name"] = streamName
    streamInformation["version"] = streamVersion
    getStreamDefinition_request.body = streamInformation.to_json

    begin
      response = agent.execute(getStreamDefinition_request)
      streamDefinition_string = ""
      response.read_body { |c| streamDefinition_string << c }
    rescue Exception => e
      #@logger.warn("Excetption Ocurred: ", :request => getStreamDefinition_request, :response => streamDefinition, :exception => e, :stacktrace => e.backtrace)
    end

    message = JSON.parse(streamDefinition_string)["message"]

    metaData_message = JSON.parse(message)["metaData"]
    correlationData_message = JSON.parse(message)["correlationData"]
    payload_message = JSON.parse(message)["payloadData"]

    streamDefinition = Hash.new
    streamDefinition["metaData"] = metaData_message
    streamDefinition["correlationData"] =correlationData_message
    streamDefinition["payloadData"] = payload_message

    return streamDefinition
  end

  end