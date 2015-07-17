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

module SchemaManager

  #This method returns the existing schema definition
  def SchemaManager.getSchemaDefinition(agent, url,schemaDefinition)

    processedSchemaURL = url+"?type=10&tableName="+schemaDefinition["tableName"]
    #getSchema_request = agent.get("http://localhost:9763/portal/controllers/apis/analytics.jag?type=10&tableName=LOGS")
    getSchema_request = agent.get(processedSchemaURL)
    getSchema_request.headers["Authorization"] = "Basic YWRtaW46YWRtaW4="

    schema_response = agent.execute(getSchema_request)
    puts schema_response["body"]
    new_response = ""
    schema_response.read_body { |c| new_response << c }
    return new_response
  end

  # this method alters the schema if needed
  def SchemaManager.setSchemaDefinition(agent, payload, arbitrary_map,correlation_map, schemaDefinition,currentSchemaString,url)
    processedURL = url + "?type=15&tableName="+schemaDefinition["tableName"]

    # combine the arbitrary map and the payload maps
    modifiedCorrelationMap = Hash.new
    correlation_map.each do |key , value|
      modifiedCorrelationMap["correlation_"+key] = value
    end

    # combine the arbitrary map and the payload maps
    modifiedArbitraryMap = Hash.new
    arbitrary_map.each do |key , value|
      modifiedArbitraryMap["_"+key] = value
    end

    correlation_addedColumns = modifiedArbitraryMap.merge(modifiedCorrelationMap)
    new_columns = payload.merge(correlation_addedColumns)

    #TODO come up with the better way to test if the schema is not set
    #if the schema is already there replace it
    unless (currentSchemaString.length < 5)
      current_schema = JSON.parse(currentSchemaString)["message"]

      columns = JSON.parse(current_schema)["columns"]

      # get the current columns and their types
      current_columns=Hash.new
      columns.each do |k, v|
        current_columns[k] = v["type"]
      end

      new_columns.each do |k, v|
        new_columns[k] = v.upcase
      end

      puts "comparing columns ________________________"
      puts current_columns
      puts new_columns

      # testing if the schema has changed
      if (current_columns == new_columns)
        return
      end

      # combine and create the new columns
      new_schema = new_columns.merge(current_columns)

      #else we need to create a new schema
    else
      updated_schema = Hash.new
      new_schema = new_columns
    end

    #adding other necessary fields and formating
    new_schema.each do |key, value|
      new_value = Hash.new
      new_value["type"] = value.upcase
      new_value["isIndex"] = true
      new_value["isScoreParam"] = false
      new_schema[key] = new_value
    end

    #replacing the schema map with the new one
    schema_map = {"columns" => new_schema}
    updated_schema = Hash.new
    updated_schema["columns"]=new_schema
    updated_schema["primaryKeys"] = Array.new


    setSchema_request = agent.post(processedURL)
    setSchema_request["Content-Type"] = "application/json"

    puts processedURL
    setSchema_request.body = LogStash::Json.dump(updated_schema)
    #setSchema_request.body = updated_schema.to_json
    puts "______________UPDATED SCHEMA__________"
    puts updated_schema.to_json
    setSchema_request.headers["Authorization"] = "Basic YWRtaW46YWRtaW4="
    response = agent.execute(setSchema_request)
    return response
  end

end