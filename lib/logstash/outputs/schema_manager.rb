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

  def SchemaManager.getSchemaDefinition(agent, url, schemaDefinition, authenticationHeader)

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

  def SchemaManager.setSchemaDefinition(agent, payload, arbitrary_map, correlation_map, metadata_map, schemaDefinition,
      currentSchema, url, authenticationHeader)

    processedURL = url + "?type=15&tableName="+schemaDefinition["tableName"]

    # add the "meta_" for the metaData map fields
    modifiedMetaDataMap = Hash.new
    unless metadata_map.nil?
      metadata_map.each do |key, value|
        modifiedMetaDataMap["meta_"+key] = value
      end
    end

    # add the correlation_ for the correlation map fields
    modifiedCorrelationMap = Hash.new
    unless correlation_map.nil?
      correlation_map.each do |key, value|
        modifiedCorrelationMap["correlation_"+key] = value
      end
    end

    # add the _ for arbitrary map keys
    modifiedArbitraryMap = Hash.new
    unless arbitrary_map.nil?
      arbitrary_map.each do |key, value|
        modifiedArbitraryMap["_"+key] = value
      end
    end

    metaData_and_correlation_maps = modifiedMetaDataMap.merge(modifiedCorrelationMap)
    correlation_addedColumns = modifiedArbitraryMap.merge(metaData_and_correlation_maps)
    new_columns = payload.merge(correlation_addedColumns)

    #if the schema is already there replace it
    current_schema = JSON.parse(currentSchema)["message"]
    columns = JSON.parse(current_schema)["columns"]
    primaryKeys = JSON.parse(currentSchema)["primaryKeys"]

    unless columns.size < 1
      new_columns = SchemaManager.processColumns(new_columns)

      # testing if the schema has changed
      schemaUpdated = true
      new_columns.each do |key,new_column_sub_field|

        #check if it's a payload value
        #if so don't change the values - skip
        unless payload[key].nil?
          new_columns[key] = columns[key]
          #continue with the next value
          next
        end

        #for other values other than the stream defined
        #compare them and update the schema if necessary
        unless columns[key].nil?
          column = columns[key]
          new_column_sub_field.each do |sub_key, sub_value|
            if column[sub_key] == sub_value
              next
            else
              schemaUpdated = false
              break
            end
          end
        else
          schemaUpdated = false
          break
        end
      end

      if (schemaUpdated)
        return
      end

      #add fields that were removed in the new columns ( added to the schema and then removed from the new columns)
      #so those fields won't get dumped
      columns.each do |key, value|
        if new_columns[key].nil?
          new_columns[key] = columns[key]
        end
      end

      # combine and create the new columns
      new_schema = columns.merge(new_columns)

      #else we need to create a new schema
    else
      new_schema = new_columns
    end

    #replacing the schema map with the new one
    updated_schema = Hash.new
    updated_schema["columns"]=new_schema

    unless primaryKeys.nil?
      updated_schema["primaryKeys"] = primaryKeys
    else
      updated_schema["primaryKeys"] = Array.new
    end

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

  private
  def SchemaManager.processColumns(columns)
    unless columns.nil?
      columns.each do |key, value|

        detailsMap = Hash.new

        values = value.split(" ")
        detailsMap["type"] = values[0].upcase
        detailsMap["isIndex"] = nil
        detailsMap["isScoreParam"] = nil

        values.each do |config|
          if config == "-i"
            detailsMap["isIndex"]=true
          elsif config == "-sp"
            detailsMap["isScoreParam"] = true
          end
        end

        #if indexing and scoreParam is not set, set it to false
        if detailsMap["isIndex"].nil?
          detailsMap["isIndex"] = false
        end

        if detailsMap["isScoreParam"].nil?
          detailsMap["isScoreParam"] = false
        end
        columns[key] = detailsMap
      end
    end

    return columns
  end

end