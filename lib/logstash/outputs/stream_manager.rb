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