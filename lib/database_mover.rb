require 'rubygems'
require 'mysql'
require 'terminal-table/import'

module ASEE
  class DatabaseMover

    attr_accessor :prj, :src, :cnf

    def initialize(prj, cnf, src, tgt, dry_run = false, debug = 0)
      @prj = prj 
      @src = src
      @tgt = tgt 
      @cnf = cnf 
      @dump  = cnf['defaults']['dump']
      @cmd   = cnf['defaults']['cmd']
      @admin = cnf['defaults']['admin']
      @src_db = cnf[prj][src].has_key?('database') ? cnf[prj][src]['database'] : "#{prj}_#{src}"
      @tgt_db = cnf[prj][tgt].has_key?('database') ? cnf[prj][tgt]['database'] : "#{prj}_#{tgt}"
      @src_cnf = {
        :host => cnf[prj][src]['host'],
        :database => @src_db,
        :username => cnf[prj][src]['username'],
        :password => cnf[prj][src]['password']  
      }
      @tgt_cnf = {
        :host => cnf[prj][tgt]['host'],
        :database => @tgt_db,
        :username => cnf[prj][tgt]['username'],
        :password => cnf[prj][tgt]['password']  
      }
      @src_deps = cnf[prj][src].has_key?('deps') ? cnf[prj][src]['deps'] : []
      @tgt_deps = cnf[prj][tgt].has_key?('deps') ? cnf[prj][tgt]['deps'] : []
      perform_sanity_check
      @dry_run = dry_run
      @debug = debug
      @ignore_tables = cnf[prj].has_key?('ignore_tables') ? cnf[prj]['ignore_tables'] : {}

    end

    def show_configuration
      t = table ['Configuration', 'Source', 'Target'] 
      t << ['Database', @src_cnf[:database], @tgt_cnf[:database]]
      t << ['Host', @src_cnf[:host], @tgt_cnf[:host]]
      puts t
    end

    # returns a hash, view_name => create view statement
    def get_view_defs
      con = Mysql.connect(@src_cnf[:host], @src_cnf[:username], 
        @src_cnf[:password], @src_cnf[:database])
      myputs "connected to #{@src_cnf[:database]}"
      views = con.query("select * from information_schema.views where table_schema = '#{@src_cnf[:database]}'")
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

    def refresh_views
      views_hash = get_view_defs
      con = Mysql.connect(@tgt_cnf[:host], @tgt_cnf[:username], @tgt_cnf[:password], @tgt_cnf[:database]) unless @dry_run
      views_hash.each_pair do |view_name, view_def|
        myputs "Fixing #{view_name}"
        fixed_view_def = fix_view_def(view_name, view_def)
        con.query(fixed_view_def) unless @dry_run
        myputs "Fixing #{fixed_view_def}" if @debug > 5
      end
      con.close unless @dry_run
    end

    def copy_views
      views_hash = get_view_defs
      con = Mysql.connect(@tgt_cnf[:host], @tgt_cnf[:username], @tgt_cnf[:password], @tgt_cnf[:database]) unless @dry_run
      views_hash.each_pair do |view_name, view_def|
        myputs "Copying #{view_name}"
        copy_view_def = "CREATE TABLE tmp_#{view_name} SELECT * FROM #{view_name}"
        con.query(copy_view_def) unless @dry_run
        drop_view_def = "DROP VIEW #{view_name}"
        con.query(drop_view_def) unless @dry_run
        rename_table = "RENAME TABLE tmp_#{view_name} TO #{view_name}"
        con.query(rename_table) unless @dry_run
        myputs "Copied #{fixed_view_def}" if @debug > 5
      end
      con.close unless @dry_run
    end

    # Fixes a "create view" statement to the right format for the destination db.
    def fix_view_def(view_name, view_def)
      fixed_view_def = view_def.gsub(/\A.* AS select /, "create or replace view #{view_name} as select ")
      if @src_deps.count == @tgt_deps.count
        src_view_cands = @src_deps.dup + [@src]
        tgt_view_cands = @tgt_deps.dup + [@tgt]
        src_view_cands.each_index do |idx|
          from = src_view_cands[idx] #"#{name}_#{@src.gsub('1','')}"
          to   = tgt_view_cands[idx] #"#{name}_#{@tgt.gsub('1','')}"
          fixed_view_def = fixed_view_def.gsub(from, to)
        end

        fixed_view_def
      else
        puts "Source and target dependent databases must be equal in quantity."
      end
    end

    # dumps the database using mysqldump
    def dump_db(ignore_tables = {}, override_db=nil)
      mycnf = @src_cnf.dup
      mycnf[:database] = override_db if override_db.is_a?(String)
      myputs(@src_cnf.inspect,5)
      dump_command = "#{@dump} --lock-tables=FALSE #{db_command_options(mycnf)}"
      skip_tables = ignore_tables.keys
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

    def dump_deps
      return unless @src_deps.respond_to?(:each)
      @src_deps.each do |db|
        dump_db({},db)
      end
      myputs @src_cnf.inspect
    end

    def purge_dumps
      `rm mysqldumps/*`
    end

    def load_db(override_db=nil)
      myputs @src_cnf.inspect if @debug
      mycnf = @tgt_cnf.dup
      mycnf[:database] = override_db if override_db.is_a?(String)
      perform_sanity_check(mycnf)
      command = "#{@admin} #{db_command_options(mycnf, false)} create #{mycnf[:database]}"
      mysys(command)
      
      src_db = override_db.is_a?(String) ? override_db.gsub(@tgt,@src) : @src_cnf[:database]
      command = "#{@cmd} #{db_command_options(mycnf)} < mysqldumps/#{src_db}.sql"
      mysys(command)
    end

    def load_deps
      return unless @tgt_deps.respond_to?(:each)
      @tgt_deps.each do |db|
        load_db(db)
      end
    end

    def db_command_options(db_hash, include_database=true)
      include_password = !(db_hash[:password].nil? || db_hash[:password] == "")
      p = include_password ? "-p#{db_hash[:password]}" : ""
      d = include_database ? db_hash[:database] : ""
      "-h #{db_hash[:host]} -u #{db_hash[:username]} #{p} #{d}"
    end

    def perform_sanity_check(tgt_cnf=nil)
      mycnf = tgt_cnf.nil? ? @tgt_cnf.dup : tgt_cnf
      unless mycnf[:database] =~ /_development/
        puts "\n\n\n\t\t*** WARNING ***\n\n\t\tYou have selected a target other than development (#{mycnf[:database]}).\n\t\tWaiting 5 seconds to continue..."
        sleep(5)
      end
      if mycnf[:database] =~ /_production/
        raise "The destination database appears to be production!  Is that really what you want?"
      end
    end

    def mysys(cmd)
      puts cmd if @debug
      `#{cmd}` unless @dry_run  
    end

    def myputs(str,dbg=5)
      return unless @debug >= dbg
      puts str
      puts "\n"
    end

  end
end
