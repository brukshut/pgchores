#!/usr/bin/env ruby

##
## /var/postgres/bin/pgchores.rb
## common postgres chores
##

class PgChores
  require 'pg'
  require 'open3'
  require 'optparse'
  require 'nagios_nrdp'
  require 'pp'
  
  def initialize(db_name)
    @db_name  = db_name.to_s
    @connection = begin
      connection = PG::Connection.open(
        :host   => 'localhost',
        :port   => '5432',
        :dbname => @db_name,
        :user   => 'postgres'
      )
    rescue PG::ConnectionBad => e
      raise e
      exit
    end
  end

  def pause_wal_replay()
    result = @connection.exec_params('SELECT pg_xlog_replay_pause();')
    result[0]['pg_xlog_replay_pause']
  end

  def resume_wal_replay()
    result = @connection.exec_params('SELECT pg_xlog_replay_resume();')
    result[0]['pg_xlog_replay_resume']
  end

  def check_wal_replay()
    result = @connection.exec_params('SELECT pg_is_xlog_replay_paused();')
    result[0]['pg_is_xlog_replay_paused']
   end

  def check_recovery()
    result = @connection.exec_params('SELECT pg_is_in_recovery();')
    result[0]['pg_is_in_recovery']
  end

  def drop_repack_extension()
    begin
      result = @connection.exec_params('DROP EXTENSION pg_repack CASCADE;')
    rescue PG::UndefinedObject
      puts 'pg_repack extension does not exist.'
    end
  end

  def create_repack_extension()
    begin
      result = @connection.exec_params('CREATE EXTENSION pg_repack;')
    rescue PG::DuplicateObject
      puts 'pg_repack extension already exists.'
    end
  end

  ## nagios nrdp
  def nrdp(servicename, state, output)
    hostname = %x(/bin/hostname -f).chomp
    subnet = hostname.split('.')[1]
    url = "http://nagios.domain/nrdp/"
    token = 'MYNRDPTOKEN'
    ## connect to nagios nrdp
    begin
      nrdp = Nagios::Nrdp.new(url: url, token: token)
    rescue Exception => e
      puts e
      exit
    end
    ## submit check
    nrdp.submit_check(hostname: hostname, servicename: "#{servicename}", state: state.to_i, output: "#{output}")
  end

  ## fetch daily dump from s3
  def fetch_daily_dump(schema)
    logfile = File.open('/var/log/postgresql/refresh.log', 'a')
    datestamp = (Time.now).strftime('%m%d%y')
    bucket_name = "backup.production.foobar.com"
    refresh_dir = '/var/postgres/db/refresh'
    dump_file = "#{schema}.UTF8.#{datestamp}_0730.dmp"
    path = "postgresql/#{Time.now.strftime('%Y')}/#{Time.now.strftime('%m')}/#{dump_file}"
    logfile.puts "Fetching #{bucket_name}/#{path}"
    logfile.flush
    Dir.mkdir(refresh_dir) unless Dir.exists?(refresh_dir)
    Dir.chdir(refresh_dir)
    unless File.exists?("#{refresh_dir}/#{dump_file}")
      s3cmd = "/usr/local/bin/s3cmd get --force s3://#{bucket_name}/#{path} #{dump_file}"
      pid_exit = run_cmd(s3cmd, logfile)
    end
    dump_info = {
      'dump_file' => dump_file,
      'dump_size' => File.stat(dump_file).size
    }
  end

  ## extract single table dump file, from daily dump file
  def extract_table(dump_file, table)
    logfile = File.open('/var/log/postgresql/refresh.log', 'a')
    logfile.puts "extracting #{table} from #{dump_file}..."
    logfile.flush
    Dir.chdir('/var/postgres/db/refresh')
    Dir.mkdir('dump') unless Dir.exists?('dump')
    extract_cmd = "/usr/bin/pg_restore --data-only --table=#{table} #{dump_file} > dump/#{table}.dmp"
    pid_exit = run_cmd(extract_cmd, logfile)
  end

  def truncate_table(db_name, table)
    logfile = File.open('/var/log/postgresql/refresh.log', 'a')
    logfile.puts "truncating #{table}..."
    logfile.flush
    truncate_cmd = "/usr/bin/psql -d #{db_name} -c 'truncate table #{table}'"
    pid_exit = run_cmd(truncate_cmd, logfile)
  end

  ## load single table from pg compressed dump file
  def load_table(db_name, table)
    logfile = File.open('/var/log/postgresql/refresh.log', 'a')
    logfile.puts "loading #{table}.dmp..."
    logfile.flush
    load_cmd = "/usr/bin/psql -d #{db_name} < dump/#{table}.dmp"
    run_cmd(load_cmd, logfile)
  end

  ## refresh a generic sequence for id
  def refresh_id_seq(db_name, table)
    logfile = File.open('/var/log/postgresql/refresh.log', 'a')
    logfile.puts "refreshing sequence for #{table}..."
    logfile.flush
    sql = "\"SELECT setval('#{table}_id_seq', COALESCE((SELECT MAX(id)+1 FROM #{table}), 1), false);\""
    refresh_cmd = "/usr/bin/psql -d #{db_name} -c #{sql}"
    run_cmd(refresh_cmd, logfile)
  end
  
  ## run shell command, write stdout and stderr to log file
  def run_cmd(cmd, logfile)
    STDOUT.sync = true
    logfile = File.open(logfile, 'a')
    logfile.puts ["#{Time.now.utc.strftime('%Y-%m-%d %T.%3N')} ", cmd].join
    logfile.flush
    pid_exit = Open3.popen2e(cmd) do |stdin, stdout_stderr, wait_thread|
      ## spawn another thread to watch the execution thread stdout/err
      Thread.new do
        stdout_stderr.each do |line|
          logfile.puts ["#{Time.now.utc.strftime('%Y-%m-%d %T.%3N')} ", line].join
          logfile.flush
        end
      end
      ## value method will return after shell command is run
      logfile.puts ["#{Time.now.utc.strftime('%Y-%m-%d %T.%3N')} ", wait_thread.value].join
      logfile.flush
      wait_thread.value
    end
    logfile.close
    pid_exit
  end    
  
  ## take dump using pg_dump, return dump_info hash
  def take_dump(dbname, schema, encoding, type)
    logfile = File.open('/var/log/postgresql/dump.log', 'a')
    #timestamp = (Time.now).strftime('%m%d%y_%H%M') if type == 'hourly'
    timestamp = (Time.now).strftime('%m%d%y_%H%M')
    dump_file = "/var/postgres/db/backup/#{dbname}.#{schema}.#{encoding}.#{timestamp}.dmp"

    ## conditional table exclude based on type?
    dump_cmd = "/usr/bin/pg_dump -v -E#{encoding} " \
               "--schema=#{schema} " \
               "--no-unlogged-table-data " \
               "-T public.bigtable -T public.staletable" \
               "-Upostgres -Fc " \
               "-f #{dump_file} #{dbname}"
    ## run command
    pid_exit = run_cmd(dump_cmd, logfile)
    ## return dump_info regardless if dump is successful
    dump_info = {
      'dump_file'   => dump_file,
      'dump_size'   => File.stat(dump_file).size,
      'exit_status' => pid_exit.exitstatus
    }
  end

  def repack_table(table)
    logfile = File.open('/var/log/postgresql/repack.log', 'a')
    repack_cmd = "/usr/bin/pg_repack -t #{table} " \
                 "--no-kill-backend " \
                 "--wait-timeout=120 " \
                 "--jobs=4 " \
                 "-d #{@db_name} -E debug"
    ## run command
    pid_exit = run_cmd(repack_cmd, logfile)
  end

  def flexible_freeze(database)
    logfile = File.open('/var/log/postgresql/freeze.log', 'a')
    freeze_cmd = "/usr/bin/python2 /var/postgres/bin/flexible_freeze.py " \
                 "--minutes 60 " \
                 "--freezeage 1000000 " \
                 "--pause 30 " \
                 "-d #{database} -v"
    ## run command
    pid_exit = run_cmd(freeze_cmd, logfile)
  end

  ## upload dump to s3
  ## requires /var/postgres/.s3cfg
  def upload_dump(dump_file)
    dump_size = File.stat(dump_file).size
    logfile = File.open('/var/log/postgresql/dump.log', 'a')
    bucket = 'backup.production.foobar.com'
    remote_path = "s3://#{bucket}/postgresql/#{Time.now.strftime('%Y')}/#{Time.now.strftime('%m')}/#{File.basename(dump_file)}"
    ## run command
    upload_cmd = "/usr/local/bin/s3cmd put #{dump_file} #{remote_path} --multipart-chunk-size-mb=15 --progress"
    pid_exit = run_cmd(upload_cmd, logfile)
    ## note size of remote dump
    s3_dump_size = %x(/usr/local/bin/s3cmd du #{remote_path}).split(' ').first.to_i
    upload_info = {
      'local_path'  => dump_file,
      'remote_path' => remote_path,
      'local_size'  => dump_size,
      'remote_size' => s3_dump_size,
      'exit_status' => pid_exit.exitstatus
    }
  end
end
