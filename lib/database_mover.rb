require 'rubygems'
require 'mysql'
require 'terminal-table/import'

module ASEE
  class DatabaseMover

    attr_accessor :prj, :src, :cnf

    def initialize(prj, cnf, src, tgt, dry_run = false)
      @prj = prj 
      @src = src
      @tgt = tgt 
      @cnf = cnf 
      @dump  = cnf['defaults']['dump']
      @cmd   = cnf['defaults']['cmd']
      @admin = cnf['defaults']['admin']
      @src_cnf = {
        :host => cnf[prj][src]['host'],
        :database => "#{prj}_#{src}",
        :username => cnf[prj][src]['username'],
        :password => cnf[prj][src]['password']  
      }
      @tgt_cnf = {
        :host => cnf[prj][tgt]['host'],
        :database => "#{prj}_#{tgt}",
        :username => cnf[prj][tgt]['username'],
        :password => cnf[prj][tgt]['password']  
      }
      @deps = ['applicants', "#{@prj}_awards", 'universities']
      perform_sanity_check
      @dry_run = dry_run

      @debug = 1
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

    # Fixes a "create view" statement to the right format for the destination db.
    def fix_view_def(view_name, view_def)
      fixed_view_def = view_def.gsub(/\A.* AS select /, "create or replace view #{view_name} as select ")
      @deps.each do |name|
        from = "#{name}_#{@src}"
        to   = "#{name}_#{@tgt}"
        fixed_view_def = fixed_view_def.gsub(from, to)
      end
      fixed_view_def
    end

    # dumps the database using mysqldump
    def dump_db(ignore_tables = {}, override_db=nil)
      mycnf = @src_cnf.dup
      mycnf[:database] = override_db if override_db.is_a?(String)
      myputs(@src_cnf.inspect,5)
      dump_command = "#{@dump} #{db_command_options(mycnf)}"
      ignore_tables.each_key do |view_name|
        dump_command += " --ignore-table=#{mycnf[:database]}.#{view_name}"
      end
      `mkdir -p mysqldumps`
      dump_command += " -r mysqldumps/#{mycnf[:database]}.sql"
      mysys(dump_command)
    end

    def dump_deps
      @deps.each do |dep_db|
        db = "#{dep_db}_#{@src}"
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
      @deps.each do |dep_db|
        db = "#{dep_db}_#{@tgt}"
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
        puts "\n\n\n\t\t*** WARNING ***\n\n\t\tYou have selected a target other than development (#{mycnf[:database]}).\n\t\tWaiting 10 seconds to continue..."
        sleep(10)
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
