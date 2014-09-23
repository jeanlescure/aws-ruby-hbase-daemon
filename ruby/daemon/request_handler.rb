$LOAD_PATH.unshift File.join("/usr/lib/ruby/gems/1.8/gems/tilt-1.4.1", "lib")
$LOAD_PATH.unshift File.join("/usr/lib/ruby/gems/1.8/gems/rack-1.5.2", "lib")
$LOAD_PATH.unshift File.join("/usr/lib/ruby/gems/1.8/gems/rack-protection-1.5.3", "lib")
$LOAD_PATH.unshift File.join("/usr/lib/ruby/gems/1.8/gems/sinatra-1.4.5", "lib")

require 'sinatra/base'
require 'json'

class RequestHandler < Sinatra::Base
  set :bind, '0.0.0.0'
  set :port, 9090
  
  before do
    content_type 'application/json'
  end
  
  
  # Read
  
  post '/test' do
    puts "!!!!"
    params[:items]
  end

  get '/req' do
    #$requests[params[:query]] = [] if $requests[params[:query]].nil?
    #stream(:keep_open) { |out| $requests[params[:query]] << out }

    #$requests[params[:query]].reject!(&:closed?)

    $admin.list().inspect
  end
  
  get '/count' do
    table = $hbase.table(params[:t], $formatter)
    result = [table.count()]
    
    result.to_json
  end
  
  get '/get' do
    get_table = $hbase.table(params[:t], $formatter)
    
    results = []
    opts = {}
    opts['LIMIT'] = params[:limit].to_i unless params[:limit].nil?
    opts['COLUMNS'] = params[:cols].split(',') unless params[:cols].nil?
    scan_result = get_table.scan(opts)
    
    scan_result.each do |key, value|
      formatted = scan_format_loop(value)
      formatted['id'] = key
      results << formatted
    end
    
    results.to_json
  end
  
  def scan_format_loop(scan_hash)
    result = {}
    scan_hash.each do |key, value|
      key_arr = key.split(':')
      if key_arr.length > 1
        nested_value(result, *key_arr, value.sub(/timestamp=\d{1,}, value=/,''))
      else
        result[key] = value.sub(/timestamp=\d{1,}, value=/,'')
      end
    end
    
    result
  end
  
  def nested_value(hash, *keys, last_key, value)
    result = keys.inject(hash) { |r, k| r[k] = {} if r[k].nil?; r[k] }
    result[last_key] = value
  end
  
  
  # Write
  
  post '/insert' do
    idx_table = $hbase.table('table_indices', $formatter)
    
    ins_table = $hbase.table(params[:t], $formatter)
    
    items_json = params[:items]
    items_hash = JSON.parse(items_json)
    
    total = items_hash.length
    total_chunk = ((total / 100) > 1) ? (total / 100) : 1
    progress_chunk = ((total / 10) > 1) ? (total / 10) : 1
    current = 0
    progress = ["Inserting items: <"]
    items_hash.each do |i_hash|
      progress << "=" if current % progress_chunk == 0
      if current % total_chunk == 0
        print "\r"
        print "#{progress.join} #{((current.to_f / total.to_f) * 100).to_i}%"
        $stdout.flush
      end
      current += 1
      idx = idx_table.incr(params[:t],'index',1)
      key = i_hash.keys[0]
      ins_table.put(idx, "#{key}:#{key}_json", i_hash.to_json)
      save_loop(key, i_hash[key], ins_table, idx)
    end
    
    progress << ">"
    print "\r"
    print "#{progress.join} 100%"
    puts
    $stdout.flush
    
    JSON.generate ["done"]
  end
  
  def save_loop(parent, rowHash, table, row)
    rowHash.each do |key, value|
      if value.is_a?(Hash) 
        save_loop("#{parent}:#{key}", value, table, row)
      else
        table.put(row, "#{parent}:#{key}", value)
      end
    end
  end
  
  get '/drop' do
    $admin.disable(params[:t])
    $admin.drop(params[:t])
  end
  
  get '/truncate' do
    $admin.truncate(params[:t])
  end
  
  get '/create' do
    args = []
    args << params[:t]
    args << params[:f1] unless params[:f1].nil?
    args << params[:f2] unless params[:f2].nil?
    args << params[:f3] unless params[:f3].nil?
    start_time = Time.now
    $admin.create(*args)
    end_time = Time.now
    
    "#{(end_time - start_time) * 1000} ms"
  end
end