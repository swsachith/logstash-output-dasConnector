module SchemaManager

  def SchemaManager.setSchemaDefinition(agent,payload,arbitrary_map)
    getSchemaResponse_string = '{"tableName":"TEST","schema":{"columns":{"col1":{"type":"STRING", "isIndexed":true}}}}'
    current_schema = JSON.parse(getSchemaResponse_string)

    schema= current_schema["schema"]
    columns = schema["columns"]

    # get the current columns and their types
    current_columns=Hash.new
    columns.each do |k, v|
      current_columns[k] = v["type"]
    end

    #TODO check if it's null then set the default schema

    # combine the arbitrary map and the payload maps
    new_columns = payload.merge(arbitrary_map)

    # testing if the schema has changed
    if(current_columns == new_columns)
      return
    end

    # combine and create the new columns
    new_schema = new_columns.merge(current_columns)

    #adding isIndexed = true to all
    new_schema.each do |key, value|
      new_value = Hash.new
      new_value["type"] = value.upcase
      new_value["isIndexed"] = "true"
      new_schema[key] = new_value
    end

    # getting the updated schema
    updated_schema = current_schema
    updated_schema.delete("schema")

    #replacing the old schema with the new one
    schema_map = {"columns" => new_schema}
    updated_schema["schema"] = schema_map

    return updated_schema
  end
end