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
      require_relative "request_handler"
      
      RequestHandler.run!
    end
    
    class ResponseHandler
      
    end
  end
end