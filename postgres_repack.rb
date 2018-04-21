#!/usr/bin/env ruby

##
## postgres_repack.rb
## script to repack postgresql tables.
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

## list of tables to repack
tables = %w(fruits grains meats vegetables juice milk cheese)

## repack tables, ensure we are running on master
chores = PgChores.new(options[:dbname])
if chores.check_recovery == 'f'
  tables.each do |table|
    chores.nrdp('Repack Status', 1, "WARNING: pg_repack of #{table} in progress")
    chore = chores.repack_table(table)
    unless chore.exitstatus == 0
      chores.nrdp('Repack Status', 2, "CRITICAL: pg_repack of #{table} failed")
      chores.drop_repack_extension
      chores.create_repack_extension
      exit
    end
  end
  chores.nrdp('Repack Status', 0, ["OK: successfully repacked tables ", tables.join(', '), '.'].join)
elsif chores.check_recovery == 't'
  chores.nrdp('Repack Status', 0, "OK: we are the slave.")
end

