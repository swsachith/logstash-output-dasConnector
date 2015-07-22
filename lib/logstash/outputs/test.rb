=begin
{"timestamp"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>true},
 "message"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>false},
 "correlation_activity_id"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>false},
 "_syslog_program"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>false},
 "host"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>true},
 "_syslog_name"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>false},
 "type"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>true},
 "_syslog_pid"=>{"type"=>"STRING", "isScoreParam"=>false, "isIndex"=>false}}


 "timestamp"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
 "message"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
 "correlation_activity_id"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false}}
 "_syslog_program"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
{"host"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
 "_syslog_name"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
 "type"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
 "_syslog_pid"=>{"type"=>"STRING", "isIndex"=>false, "isScoreParam"=>false},
=end

columns = Hash.new

details1 = Hash.new
details1["type"] = "String"
details1["isIndex"] = true
details1["isScoreParam"] = true
columns["host"] = details1

new_columns = Hash.new

details = Hash.new
details["type"] = "String"
details["isScoreParam"] = false
details["isIndex"] = true
new_columns["host"] = details

schemaUpdated = true
columns.each do |key,value|
  unless new_columns[key].nil?
    new_column = new_columns[key]
    value.each do |key, subvalue|
      if new_column[key] == subvalue
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

puts schemaUpdated