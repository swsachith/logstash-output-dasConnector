module DASUtils
  #This method returns the existing schema definition
  def DASUtils.getStreamDefinition(agent,schemaDefinition)
    getSchema_request = Hash.new
    getSchema_request = agent.get(schemaDefinition["schemaURL"])

    schema_response = agent.execute(getSchema_request)
    response = ""
    schema_response.read_body { |c| response << c }

    return response
  end

end