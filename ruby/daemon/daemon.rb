module Hbase
  class Daemon
    def initialize(hbase, admin, formatter)
      $requests = {}
      $hbase = hbase
      $admin = admin
      $formatter = formatter
    end
    
    def requests
      $requests
    end
    
    def start
      require_relative "utils"
      require_relative "hbase_handler"
      require_relative "request_handler"
      
      RequestHandler.set :port => ENV['RB_DAEMON_PORT']
      RequestHandler.run!
    end
    
    class ResponseHandler
      
    end
  end
end