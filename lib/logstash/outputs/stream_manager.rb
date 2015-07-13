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

  # This method sets the stream definition
  # params
  # example stream definition
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
  def StreamManager.addStreamDefinition(agent, provided_streamDefinition, payloadFields, url)
    streamDefinition = Hash.new
    streamDefinition = provided_streamDefinition
    streamDefinition["payloadData"] = payloadFields
    addStreamRequest = agent.post(url)

    #dumping the payload as a json
    addStreamRequest.body = LogStash::Json.dump(streamDefinition)

    addStream_response = agent.execute(addStreamRequest)

    # Consume body to let this connection be reused
    #rbody = ""
    #response.read_body { |c| rbody << c }

    return addStreamRequest.body
  end

end