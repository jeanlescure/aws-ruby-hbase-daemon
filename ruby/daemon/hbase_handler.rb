require 'json'

module Hbase
  class Handler
    include HbaseHandlerUtils
    
    def handle(query)
      return nil if query.nil?
      
      send(query[:query_type].to_s, query[:query_hash])
    end
    
    def gen_index_if_missing
      $admin.create('table_indices','index') unless $admin.exists? 'table_indices'
    end
    
    def gen_fields_if_missing
      $admin.create('table_fields','fields') unless $admin.exists? 'table_fields'
    end
    
    def gen_creates_if_missing
      $admin.create('table_creates','create') unless $admin.exists? 'table_creates'
    end
    
    def update_fields(table_name,columns)
      fields_table = $hbase.table('table_fields', $formatter)
      current_fields = fields_table.get(table_name)
      if current_fields.nil?
        updated_fields = columns
      else
        current_fields = JSON.parse(current_fields['fields:'].sub(/timestamp=\d{1,}, value=/,''))
        updated_fields = current_fields + (columns - current_fields)
      end
      fields_table.put(table_name,'fields',JSON.generate(updated_fields)) unless current_fields == updated_fields
    end
    
    def create_table(qhash)
      gen_creates_if_missing
      creates_table = $hbase.table('table_creates', $formatter)
      creates_table.put(qhash[:table], 'create', qhash[:create_sentence])
      
      gen_fields_if_missing
      update_fields(qhash[:table],qhash[:columns])
    
      args = [qhash[:table],qhash[:table]]
      gen_index_if_missing
      $admin.create(*args)
      Result.generate()
    end
    
    def insert(qhash)
      raise "Table does not exist!" unless $admin.exists? qhash[:into]
      
      idx_table = $hbase.table('table_indices', $formatter)
      
      ins_table = $hbase.table(qhash[:into], $formatter)
      
      items_arr = qhash[:values]
      
      total = items_arr.length * items_arr[0].length
      total_chunk = ((total / 100) > 1) ? (total / 100) : 1
      progress_chunk = ((total / 10) > 1) ? (total / 10) : 1
      current = 0
      progress = ["Inserting items: <"]
      
      gen_fields_if_missing
      update_fields(qhash[:into], qhash[:columns])
      
      items_arr.each do |i_arr|
        progress << "=" if current % progress_chunk == 0
        if current % total_chunk == 0
          print "\r"
          print "#{progress.join} #{((current.to_f / total.to_f) * 100).to_i}%"
          $stdout.flush
        end
        idx = idx_table.incr(qhash[:into],'index',1)
        i_arr.each_with_index do |value, index|
          ins_table.put(idx, "#{qhash[:into]}:#{qhash[:columns][index]}", value)
          current += 1
        end
      end
      
      progress << ">"
      print "\r"
      print "#{progress.join} 100%"
      puts
      $stdout.flush
      
      nil
    end
    
    def update(qhash)
      raise "Table does not exist!" unless $admin.exists? qhash[:update]
      
      update_table = $hbase.table(qhash[:update], $formatter)
      
      ids = qhash[:where].map { |val| val[:value] if val[:column] == 'id' }
      
      ids.each do |idx|
        qhash[:set].each do |col_val_pair|
          update_table.put(idx, "#{qhash[:update]}:#{col_val_pair[:column]}", col_val_pair[:value])
        end
      end unless (ids.length == 1 && ids[0].nil?)
      
      nil
    end
    
    def select(qhash)
      results = []
      opts = {}
      opts['LIMIT'] = qhash[:limit].to_i unless qhash[:limit].nil?
      opts['COLUMNS'] = qhash[:select][:columns].map{ |v| "#{qhash[:from]}:#{v}" } if has_column_list?(qhash)
      
      
      qhash[:from].each do |from_table|
        get_table = $hbase.table(from_table, $formatter)
        scan_result = get_table.scan(opts)
        
        scan_result.each do |key, value|
          formatted = scan_format_loop(value)
          formatted['id'] = key
          results << formatted
        end
      end
      
      result_from_scan_results(results)
    end
    
    def show(qhash)
      unless qhash[:show] == 'create'
        send("show_#{qhash[:show]}", qhash[:from], qhash[:like], qhash[:where])
      else
        send("show_#{qhash[:show]}", qhash[:table_or_db], qhash[:name])
      end
    end
    
    def show_create(table_or_db, name)
      begin
        creates_table = $hbase.table('table_creates', $formatter)
        create_sentence = creates_table.get(name)['create:'].sub(/timestamp=\d{1,}, value=/,'')
        
        Result.generate(['Table', 'Create Table'],[[name,create_sentence]])
      rescue Java::OrgApacheHadoopHbase::TableNotFoundException, NoMethodError
        Result.generate()
      end
    end
    
    def show_fields(from, like, where)
      begin
        fields_table = $hbase.table('table_fields', $formatter)
        fields = JSON.parse(fields_table.get(from)['fields:'].sub(/timestamp=\d{1,}, value=/,''))
        fields.map! { |col| [col, '', nil, 'YES', nil, nil, nil, 'select,insert,update,references', ''] }
        Result.generate(['Field', 'Type', 'Collation', 'Null', 'Key', 'Default', 'Extra', 'Privileges', 'Comment'],fields)
      rescue Java::OrgApacheHadoopHbase::TableNotFoundException, NoMethodError
        Result.generate()
      end
    end
    
    def show_tables(from, like, where)
      like = ".*" if like.nil?
      tables = $admin.list(like).map { |table_name| [table_name] }
      tables = tables - [['table_indices'], ['table_fields'], ['table_creates']]
      if tables.length > 0
        Result.generate(['Tables_in_adapter'],tables)
      else
        Result.generate()
      end
    end
    
    
    
    def scan_format_loop(scan_hash)
      result = {}
      scan_hash.each do |key, value|
        result[key.gsub(/.{1,}:/,'')] = value
      end
      result
    end
    
    def result_from_scan_results(results)
      rows = []
      results.each do |row_hash|
        row_arr = []
        row_hash.values.each do |value|
          row_arr << value.sub(/timestamp=\d{1,}, value=/,'')
        end
        rows << row_arr
      end
      begin
        Result.generate(results[0].keys, rows)
      rescue NoMethodError
        Result.generate()
      end
    end
    
    def scan_nested_loop(scan_hash)
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
    
    def method_missing(method, *args, &block)
      puts "!!!!!!!!! #{method} is undefined !!!!!!!!!!"
      Result.generate()
    end
  end
  
  class Result < Hash
    def self.generate(columns=[], rows=[])
      {
        :fields => columns,
        :results => rows
      }
    end
  end
end