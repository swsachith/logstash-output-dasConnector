require 'json'
test = JSON.parse('{"columns":{"type":{"type":"STRING","isScoreParam":false,"isIndex":true}},"primaryKeys":[]}')

puts test
puts test["columns"]