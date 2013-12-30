require 'rubygems'
require 'mysql'
require 'terminal-table/import'

class DatabaseMover

  def initialize(src_conf, tgt_conf, env_conf = {}, dry_run = false)
    # these inform which command line commands to use to perform operations
    @dump  = env_conf['dump']
    @cmd   = env_conf['cmd']
    @admin = env_conf['admin']

    # source/target variables
    @src_conn = {
      :host => src_conf['host'],
      :username => src_conf['username'],
      :password => src_conf['password']
    }
    @tgt_conn = {
      :host => tgt_conf['host'],
      :username => tgt_conf['username'],
      :password => tgt_conf['password']
    }

    # the difference between the main database and dependencies is superficial
    @src_dbs  = [src_conf['database']]
    @src_dbs += src_conf['dependencies'] if src_conf.has_key?('dependencies')
    @tgt_dbs  = [tgt_conf['database']]
    @tgt_dbs += tgt_conf['dependencies'] if tgt_conf.has_key?('dependencies')

    @database_map = {}
    @src_dbs.each_with_index{|db,idx| @database_map[db] = @tgt_dbs[idx]}


    # try to stop users from doing something nuts like writing over production
    perform_sanity_check

    # environment variables
    @dry_run = dry_run
    @ignore_tables = env_conf.has_key?('ignore_tables') ? env_conf['ignore_tables'] : {}

    # internal variables
    @views_hash = {}
    # @debug = true
  end


  #
  # dump all databases
  #
  def dump_dbs
    @src_dbs.each do |db|
      dump_db(db) # do not dump views
    end
  end


  #
  # load all databases from dumps
  #
  def load_dbs
    @database_map.each do |src_db, tgt_db|
      load_db(src_db, tgt_db)
    end
  end




  #
  # run `create view` sql
  #
  def create_views
    @database_map.each do |src_db, db|
      puts "creating views on #{db}"
      views_hash = view_defs(src_db)
      next if views_hash.empty?

      con = Mysql.connect(@tgt_conn[:host], @tgt_conn[:username], @tgt_conn[:password], db) unless @dry_run

      # some views depend on others existing, so we may need to iterate through
      # more than once
      new_view_created = true
      views_to_do = views_hash.dup

      while new_view_created && !views_to_do.empty?
        skipped_views = {}
        new_view_created = false

        views_to_do.each_pair do |view_name, view_def|
          # prep view SQL
          fixed_view_def = view_def.gsub(/^.* AS (.?select) /i, "create or replace view `#{view_name}` as \\1 ")

          # iterate through all databases in order to find a suitable database to
          # create the view into
          @database_map.each do |src_fix, tgt_fix|
            fixed_view_def.gsub!(src_fix, tgt_fix)
          end

          begin
            con.query(fixed_view_def) unless @dry_run
            new_view_created = true
          rescue
            skipped_views[view_name] = view_def
          end

        end

        views_to_do = skipped_views
      end

      if !skipped_views.empty?
        puts "\n\n****** One or more views could not be created on #{db}: ****"
        puts skipped_views.keys.join("\n")
      end

      con.close unless @dry_run
    end
  end

  #
  # creates tables instead of views and loads the current contents
  #
  def snapshot_views
    create_views

    @database_map.each do |src_db, db|
      puts "snapshotting views on #{db}"
      views_hash = view_defs(src_db)
      next if views_hash.empty?

      con = Mysql.connect(@tgt_conn[:host], @tgt_conn[:username], @tgt_conn[:password], db) unless @dry_run
      views_hash.each_pair do |view_name, view_def|
        copy_view_def = "CREATE TABLE tmp_#{view_name} SELECT * FROM #{view_name}"
        con.query(copy_view_def) unless @dry_run
        drop_view_def = "DROP VIEW #{view_name}"
        con.query(drop_view_def) unless @dry_run
        rename_table = "RENAME TABLE tmp_#{view_name} TO #{view_name}"
        con.query(rename_table) unless @dry_run
      end
      con.close unless @dry_run
    end
  end


  #
  # toss mysql dump files
  #
  def purge_dumps
    `rm mysqldumps/*`
  end


  #
  # gets a list of views for each database
  #
  def get_view_defs
    @src_dbs.map{|db| view_defs(db)}
  end



protected


  #
  # dumps the database using mysqldump
  #
  def dump_db(db)
    puts "Dumping #{db}"
    mycnf = @src_conn.dup
    mycnf[:database] = db

    dump_command = "#{@dump} --lock-tables=FALSE #{db_command_options(mycnf)}"
    skip_tables = view_defs(db).keys
    unless @ignore_tables.empty?
      skip_tables = skip_tables | @ignore_tables 
    end

    skip_tables.each do |view_name|
      dump_command += " --ignore-table=#{mycnf[:database]}.#{view_name}"
    end

    `mkdir -p mysqldumps`
    dump_command += " -r mysqldumps/#{mycnf[:database]}.sql"
    mysys(dump_command)
  end


  #
  # load data tables into the target db (does not include views)
  #
  def load_db(src_db, tgt_db)
    puts "Loading #{src_db} into #{tgt_db}"
    mycnf = @tgt_conn.dup
    mycnf[:database] ||= tgt_db

    perform_sanity_check
    if mycnf[:database] =~ /production$/
      raise "It looks like you're trying to drop a table from production. Be careful!!!"
    end

    # really playing with fire here...
    command = "#{@admin} #{db_command_options(mycnf, false)} -f drop #{mycnf[:database]}"
    mysys(command)

    command = "#{@admin} #{db_command_options(mycnf, false)} create #{mycnf[:database]}"
    mysys(command)
    
    command = "#{@cmd} #{db_command_options(mycnf)} < mysqldumps/#{src_db}.sql"
    mysys(command)
  end


  #
  # returns a hash, view_name => create view statement
  #
  def view_defs(db)
    @views_hash[db] ||= begin
      con = Mysql.connect(@src_conn[:host], @src_conn[:username], 
        @src_conn[:password], db)
      myputs "connected to #{@src_conn[:database]}"

      views = con.query("select * from information_schema.views where table_schema = '#{db}'")
      views_hash = {}
      views.each do |v|
        view_name = v[2]
        view_defs = con.query("show create view #{view_name}")
        views_hash[view_name] = view_defs.fetch_row[1]
      end
      con.close
      myputs "found #{views_hash.size} views\n"
      views_hash
    end
  end


  #
  # check for potential problems that the user will potentially regret
  #
  def perform_sanity_check
    perceived_target = @tgt_dbs.first.split('_').last

    unless perceived_target == 'development'
      puts "\n\n\n\t\t*** WARNING ***\n\n\t\tYou have selected a target other than development (#{perceived_target}).\n\t\tWaiting 5 seconds to continue...  Press Ctrl+C to abort"
      sleep(5)
    end

    if perceived_target =~ /_production/
      raise "The destination database appears to be production!  Is that really what you want?"
    end

    issue = false
    @database_map.each do |src,tgt|
      s,t = src.split('_')[0..-2].join('_'), tgt.split('_')[0..-2].join('_')
      if s != t
        puts "\n\n\n*** WARNING *** \n\nYour dependency order does not match (#{src} ---> #{tgt})."
        issue = true
      end
    end

    if issue
      puts "Waiting 5 seconds to continue...  Press Ctrl+C to abort"
      sleep 5
    end
  end


  def db_command_options(db_hash, include_database=true)
    include_password = !(db_hash[:password].nil? || db_hash[:password] == "")
    p = include_password ? "-p#{db_hash[:password]}" : ""
    d = include_database ? db_hash[:database] : ""
    "-h #{db_hash[:host]} -u #{db_hash[:username]} #{p} #{d}"
  end

  def mysys(cmd)
    puts cmd if @debug
    `#{cmd}` unless @dry_run  
  end

  def myputs(str,dbg=false)
    return unless dbg
    puts str
    puts "\n"
  end

end
