#!/usr/bin/env ruby

##
## postgres_refresh.rb
## script to refresh select postgresql tables
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

## ensure we are running on master
tables =  %w(cheese eggs fruits grains milk meats)
chores = PgChores.new(options[:dbname])
if chores.check_recovery == 'f'
  dump = chores.fetch_daily_dump('public')
  if File.exists?("/var/postgres/db/refresh/#{dump['dump_file']}")
    tables.each do |table|
      chores.extract_table(dump['dump_file'], table)
    end
  end
  ## truncate our tables to restore...
  tables.each do |table|
    chores.truncate_table(options[:dbname], table)
  end
  ## load each table
  tables.each do |table|
    chores.load_table(options[:dbname], table)
  end
  ## refreshes each table
  tables.each do |table|
    chores.refresh_id_seq(options[:dbname], table)
  end  
else
  puts "we are the slave. exiting..."
end
