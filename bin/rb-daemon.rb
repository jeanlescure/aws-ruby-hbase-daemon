# Run the java magic include and import basic HBase types that will help ease
# hbase hacking.
include Java

# Add the $HBASE_HOME/lib/ruby OR $HBASE_HOME/src/main/ruby/lib directory
# to the ruby load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift "/home/hadoop/src/main/ruby"
if File.exists?(File.join(File.dirname(__FILE__), "..", "lib", "ruby", "hbase.rb"))
  $LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib", "ruby")
else
  $LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "src", "main", "ruby")
end

#
# FIXME: Switch args processing to getopt
#
# See if there are args for this shell. If any, read and then strip from ARGV
# so they don't go through to irb.  Output shell 'usage' if user types '--help'
cmdline_help = <<HERE # HERE document output as shell usage
Usage: shell [OPTIONS] [SCRIPTFILE [ARGUMENTS]]

 --format=OPTION                Formatter for outputting results.
                                Valid options are: console, html.
                                (Default: console)

 -d | --debug                   Set DEBUG log levels.
 -h | --help                    This help.
HERE
found = []
format = 'console'
script2run = nil
#log_level = org.apache.log4j.Level::ERROR
log_level = org.apache.log4j.Level::OFF
for arg in ARGV
  if arg =~ /^--format=(.+)/i
    format = $1
    if format =~ /^html$/i
      raise NoMethodError.new("Not yet implemented")
    elsif format =~ /^console$/i
      # This is default
    else
      raise ArgumentError.new("Unsupported format " + arg)
    end
    found.push(arg)
  elsif arg == '-h' || arg == '--help'
    puts cmdline_help
    exit
  elsif arg == '-d' || arg == '--debug'
    #log_level = org.apache.log4j.Level::DEBUG
    log_level = org.apache.log4j.Level::OFF
    $fullBackTrace = true
    puts "Setting DEBUG log level..."
  else
    # Presume it a script. Save it off for running later below
    # after we've set up some environment.
    script2run = arg
    found.push(arg)
    # Presume that any other args are meant for the script.
    break
  end
end

# Delete all processed args
found.each { |arg| ARGV.delete(arg) }

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)
org.apache.log4j.Logger.getRootLogger().setLevel(log_level)

# Require HBase now after setting log levels
require 'hbase'

# Load hbase shell
require 'shell'

# Require formatter
require 'shell/formatter'

# Presume console format.
# Formatter takes an :output_stream parameter, if you don't want STDOUT.
@formatter = Shell::Formatter::Console.new

# Setup the HBase module.  Create a configuration.
@hbase = Hbase::Hbase.new
@admin = @hbase.admin(@formatter)

# Setup console
@shell = Shell::Shell.new(@hbase, @formatter)

# Include hbase constants
include HBaseConstants

#@table = @hbase.table(@formatter)
require 'daemon/daemon'

dmon = Hbase::Daemon.new(@hbase, @admin, @formatter)
dmon.start