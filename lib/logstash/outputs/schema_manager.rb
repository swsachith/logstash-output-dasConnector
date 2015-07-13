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
  def SchemaManager.getSchemaDefinition(agent, schemaDefinition)
    getSchema_request = Hash.new
    getSchema_request = agent.get(schemaDefinition["schemaURL"])

    schema_response = agent.execute(getSchema_request)
    response = ""
    schema_response.read_body { |c| response << c }
    return response
  end

  # this method alters the schema if needed
  def SchemaManager.setSchemaDefinition(agent, payload, arbitrary_map, schemaDefinition)
    getSchemaResponse_string = '{"tableName":"TEST","schema":{"columns":{"col1":{"type":"STRING", "isIndexed":true}}}}'
    #getSchemaResponse_string = ""

    # combine the arbitrary map and the payload maps
    new_columns = payload.merge(arbitrary_map)

    #TODO come up with the better way to test if the schema is not set
    #if the schema is already there replace it
    unless (getSchemaResponse_string.length < 5)
      current_schema = JSON.parse(getSchemaResponse_string)

      schema= current_schema["schema"]
      columns = schema["columns"]

      # get the current columns and their types
      current_columns=Hash.new
      columns.each do |k, v|
        current_columns[k] = v["type"]
      end

      #TODO check if it's null then set the default schema

      # testing if the schema has changed
      if (current_columns == new_columns)
        return
      end

      # combine and create the new columns
      new_schema = new_columns.merge(current_columns)

      # getting the updated schema
      updated_schema = current_schema
      updated_schema.delete("schema")


      #else we need to create a new schema
    else
      updated_schema = Hash.new
      updated_schema["tableName"] = schemaDefinition["tableName"]
      new_schema = new_columns
    end

    #adding other necessary fields and formating
    new_schema.each do |key, value|
      new_value = Hash.new
      new_value["type"] = value.upcase
      new_value["isIndexed"] = "true"
      new_value["isScoreParam"] = false
      new_schema[key] = new_value
    end

    #replacing the schema map with the new one
    schema_map = {"columns" => new_schema}
    updated_schema["schema"] = schema_map

    return updated_schema
  end

end