#!/usr/bin/env ruby

##
## postgres_freeze.rb
## wrapper around flexible_freeze.py
##
require 'pg'
require 'open3'
require 'optparse'
require 'nagios_nrdp'
require '/var/postgres/bin/pgchores.rb'

## options
options = Hash.new
opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [OPTIONS]"
  opts.on('-d DBNAME', '--dbname=DBNAME', String, 'database name') do |v|
    options[:dbname] = v
  end
  opts.on('-h', '--help', 'help') do
    puts opts
    exit
  end
end
opt_parser.parse!

## require database name
if options[:dbname].nil?
  puts opt_parser
  exit
end

## invoke flexible_freeze.py 
## ensure we are running on master
chores = PgChores.new(options[:dbname])
if chores.check_recovery == 'f'
  chores.nrdp('Freeze Status', 1, "WARNING: flexible_freeze.py is running on #{options[:dbname]}.")
  chore = chores.flexible_freeze(options[:dbname])
  unless chore.exitstatus == 0
    chores.nrdp('Freeze Status', 2, "CRITICAL: flexible_freeze.py encountered a problem.")
  end
  chores.nrdp('Freeze Status', 0, "OK: successfully ran flexible_freeze.py on #{options[:dbname]}")
elsif chores.check_recovery == 't'
  chores.nrdp('Freeze Status', 0, "OK: we are the slave.")
end

