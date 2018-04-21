#!/usr/bin/env ruby

##
## postgres_backup.rb script
## takes dumps of various schemas
## logs to /var/log/postgresql/dump.log
## uploads dump files to s3 if specified
##
require '/var/postgres/bin/pgchores.rb'
require 'optparse'

## main
options = {}
opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [OPTIONS]"
  opts.on('-d DBNAME', '--dbname=DBNAME', String, 'database name') do |v|
    options[:dbname] = v
  end
  opts.on('-e ENCODING', '--encoding=ENCODING', String, 'encoding, default is UTF8') do |v|
    options[:encoding] = v
  end
  opts.on('-p', '--pause-wal-replay', 'pause and restart wal replay before and after dump') do |v|
    options[:pause] = v
  end
  opts.on('-s SCHEMA', '--schema=SCHEMA', String, 'schema, one of public, insights or warehouse') do |v|
    options[:schema] = v
  end
  opts.on('-t TYPE', '--type=TYPE', String, 'type, hourly or daily') do |v|
    options[:type] = v
  end
  opts.on('-u', '--upload', 'upload dump file to s3') do |v|
    options[:upload] = v
  end
  opts.on('-h', '--help', 'help') do
    puts opts
    exit
  end
end
opt_parser.parse!

## require db name
if options[:dbname].nil?
  puts opt_parser
  exit
end

## set default schema and encoding
options[:encoding] = 'UTF8' if options[:encoding].nil?
options[:schema] = 'public' if options[:schema].nil?
options[:type] = 'daily' if options[:type].nil?

## main
mychores = PgChores.new(options[:dbname])
## don't run on master
if mychores.check_recovery == 'f'
  mychores.nrdp("#{options[:schema].capitalize} Schema Dump Status", 0, 'OK: We are the master')
  mychores.nrdp("#{options[:schema].capitalize} Schema Upload Status", 0, 'OK: We are the master')
## only run on the slave
elsif mychores.check_recovery == 't'
  mychores.nrdp("#{options[:schema].capitalize} Schema Dump Status", 1, "WARNING: #{options[:schema]} dump in progress...")
  ## pause and resume wal replay if desired
  mychores.pause_wal_replay if options[:pause]
  dump_result = mychores.take_dump(options[:dbname], options[:schema], 'UTF8', options[:type])
  mychores.resume_wal_replay if options[:pause]
  ## if dump is successful, notify nagios and attempt upload to s3
  if dump_result['exit_status'] == 0
     mychores.nrdp("#{options[:schema].capitalize} Schema Dump Status", 0, "OK: #{dump_result['dump_file']}.")
  else
    mychores.nrdp("#{options[:schema].capitalize} Schema Dump Status", 2, "CRITICAL: something went wrong with #{options[:schema]} dump.")
  end
  ## upload dump file to s3 if we specified upload
  if options[:upload]
    mychores.nrdp("#{options[:schema].capitalize} Upload Status", 1, "WARNING: #{options[:schema]} dump s3 upload in progress.")
    upload_result = mychores.upload_dump(dump_result['dump_file'])
    if upload_result['exit_status'] == 0
      ## verify that local file size matches remote s3 file size
      if upload_result['local_size'] == upload_result['remote_size']
        mychores.nrdp("#{options[:schema].capitalize} Schema Upload Status", 0, "OK: #{upload_result['remote_path']}")
      end
    else
      mychores.nrdp("#{options[:schema].capitalize} Schema Upload Status", 2, "CRITICAL: failed to upload #{upload_result['local_path']}.")
    end
  end
end
