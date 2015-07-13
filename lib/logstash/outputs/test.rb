require "json"
getSchemaResponse_string = '{"tableName":"TEST","schema":{"columns":{"col1":{"type":"STRING", "isIndexed":true},"col2":{"type":"INT", "isIndexed":true}}}}'
current_schema = JSON.parse(getSchemaResponse_string)

schema= current_schema["schema"]
columns = schema["columns"]
puts columns
puts columns.size

current_columns=Hash.new
columns.each do |k,v|
  current_columns[k] = v["type"]
end

puts current_columns

backup_current_columns = current_columns

payload = Hash.new
payload["streamId"] = "string"
payload["col1"] ="INT"

arb_attr =  Hash.new
arb_attr["syslog_timestamp"]= "STRING"

new_schema = payload.merge(arb_attr)

puts new_schema

updatedSchema = new_schema.merge(current_columns)
puts updatedSchema

#replaces stuff in the current columns
updatedSchema_2 = current_columns.merge(new_schema)
puts updatedSchema_2

#Hash Comparison test
payload = Hash.new
payload["col1"] = "STRING"
payload["col2"] ="INT"

puts payload == backup_current_columns

#adding isIndexed = true to all
new_schema.each do |key, value|
  new_value = Hash.new
  new_value["type"] = value.upcase
  new_value["isIndexed"] = "true"
  new_schema[key] = new_value
end

puts new_schema