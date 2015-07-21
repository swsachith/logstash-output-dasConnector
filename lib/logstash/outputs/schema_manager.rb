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
  #
  #agent => the web agent FTW:Agent
  #url => URL of the DAS Server
  #schemaDefinition => Schema Definition map
  #AuthenticationHeader => The authentication details

  def SchemaManager.getSchemaDefinition(agent, url,schemaDefinition,authenticationHeader)

    processedSchemaURL = url+"?type=10&tableName="+schemaDefinition["tableName"]

    getSchema_request = agent.get(processedSchemaURL)
    getSchema_request.headers["Authorization"] = authenticationHeader

    begin
      schema_response = agent.execute(getSchema_request)
    rescue Exception => e
      #@logger.warn("Excetption Ocurred: ", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end

    new_response = ""
    schema_response.read_body { |c| new_response << c }
    return new_response

  end

  # This method sets the schema if required
  #
  #agent => the web agent FTW:Agent
  #payload => map of payload fields and values
  #arbtrary_map => map of arbitrary fields and values
  #correlation_map => map of correlation fields and values
  #metadata_map => map of metadata fields and values
  #schemaDefinition => map of schema definition values

  def SchemaManager.setSchemaDefinition(agent, payload, arbitrary_map,correlation_map,metadata_map, schemaDefinition,
      currentSchema,url,authenticationHeader)

    processedURL = url + "?type=15&tableName="+schemaDefinition["tableName"]

    # add the "meta_" for the metaData map fields
    modifiedMetaDataMap = Hash.new
    metadata_map.each do |key , value|
      modifiedMetaDataMap["meta_"+key] = value
    end

    # add the correlation_ for the correlation map fields
    modifiedCorrelationMap = Hash.new
    correlation_map.each do |key , value|
      modifiedCorrelationMap["correlation_"+key] = value
    end

    # add the _ for arbitrary map keys
    modifiedArbitraryMap = Hash.new
    arbitrary_map.each do |key , value|
      modifiedArbitraryMap["_"+key] = value
    end

    metaData_and_correlation_maps = modifiedMetaDataMap.merge(modifiedCorrelationMap)
    correlation_addedColumns = modifiedArbitraryMap.merge(metaData_and_correlation_maps)
    new_columns = payload.merge(correlation_addedColumns)

    #if the schema is already there replace it
    current_schema = JSON.parse(currentSchema)["message"]
    columns = JSON.parse(current_schema)["columns"]

    unless columns.size < 1
      # get the current columns and their types
      current_columns=Hash.new
      columns.each do |k, v|
        current_columns[k] = v["type"]
      end

      new_columns.each do |k, v|
        # todo test if you need to have the uppercase
        new_columns[k] = v.upcase
      end

      # testing if the schema has changed
      if (current_columns == new_columns)
        return
      end

      # combine and create the new columns
      new_schema = new_columns.merge(current_columns)

      #else we need to create a new schema
    else
      new_schema = new_columns
    end

    #adding other necessary fields and formating
    new_schema.each do |key, value|
      new_value = Hash.new
      new_value["type"] = value.upcase
      # todo stop changing the indexing for the current schema
      # todo have some configuration to specify whether these should be indexed or not
      new_value["isIndex"] = true
      new_value["isScoreParam"] = false
      new_schema[key] = new_value
    end

    #replacing the schema map with the new one
    updated_schema = Hash.new
    updated_schema["columns"]=new_schema
    updated_schema["primaryKeys"] = Array.new


    setSchema_request = agent.post(processedURL)
    setSchema_request["Content-Type"] = "application/json"
    setSchema_request.headers["Authorization"] = authenticationHeader

    begin
      setSchema_request.body = LogStash::Json.dump(updated_schema)
      response = agent.execute(setSchema_request)
    rescue Exception => e
      #@logger.warn("Excetption Ocurred: ", :request => request, :response => response, :exception => e, :stacktrace => e.backtrace)
    end

    return response

  end

end