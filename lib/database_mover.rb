require 'rubygems'
require 'mysql'

SOURCE_DATABASE = {
  :host => 'localhost',
  :database => 'ndseg_2011_development',
  :username => 'root',
  :password => ''  
}

DESTINATION_DATABASE = {
  :host => 'localhost',
  :database => 'ndseg_development',
  :username => 'root'
}

SECONDARY_DATABASES = {
  'applicants_staging' => 'applicants_development',
  'ndseg_awards_staging' => 'ndseg_awards_development',  
  'universities_staging' => 'universities_development'}

MYSQL = 'mysql5'
MYSQLADMIN = 'mysqladmin5'

module ASEE
  class DatabaseMover

    attr_accessor :prj, :src, :cnf

    def initialize(prj, cnf, src, tgt)
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
    end

    # returns a hash, view_name => create view statement
    def get_view_defs
      source_db_hash = @src_cnf
      con = Mysql.connect(source_db_hash[:host], source_db_hash[:username], 
        source_db_hash[:password], source_db_hash[:database])
      puts "connected"
      views = con.query("select * from information_schema.views where table_schema = '#{source_db_hash[:database]}'")
      views_hash = {}
      views.each do |v|
        view_name = v[2]
        view_defs = con.query("show create view #{view_name}")
        views_hash[view_name] = view_defs.fetch_row[1]
      end
      con.close
      puts "found #{views_hash.size} views"
      views_hash
    end

    def refresh_views
      views_hash = get_view_defs
      #puts views_hash.inspect
      con = Mysql.connect(@src_cnf[:host], @src_cnf[:username], 
        @src_cnf[:password], @src_cnf[:database])
      views_hash.each_pair do |view_name, view_def|
        puts "Fixing #{view_name}"
        fixed_view_def = fix_view_def(view_name, view_def)
        puts fixed_view_def
        #con.query(fixed_view_def)
      end
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
      mycnf = @src_cnf
      mycnf[:database] = override_db if override_db.is_a?(String)
      dump_command = "#{@dump} #{db_command_options(mycnf)}"
      ignore_tables.each_key do |view_name|
        dump_command += " --ignore-table=#{mycnf[:database]}.#{view_name}"
      end
      `mkdir -p mysqldumps`
      dump_command += " -r mysqldumps/#{mycnf[:database]}.sql"
      puts dump_command
      `#{dump_command}`
    end

    def dump_deps
      @deps.each do |dep_db|
        db = "#{dep_db}_#{@src}"
        dump_db({},db)
      end
    end

    def load_db(override_db=nil)
      mycnf = @tgt_cnf
      mycnf[:database] = override_db if override_db.is_a?(String)
      perform_sanity_check(mycnf)
      command = "#{@admin} #{db_command_options(mycnf, false)} create #{mycnf[:database]}"
      #puts command
#      `#{command}`
      
      command = "#{@cmd} #{db_command_options(mycnf)} < mysqldumps/#{@src_cnf[:database]}.sql"
      puts command
      `#{command}`
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
      mycnf = tgt_cnf.nil? ? @tgt_cnf : tgt_cnf
      unless mycnf[:database] =~ /_development/
        puts "\n\n\n\t\t*** WARNING ***\n\n\t\tYou have selected a target other than development (#{mycnf[:database]}).\n\t\tWaiting 10 seconds to continue..."
        sleep(10)
      end
      if mycnf[:database] =~ /_production/
        raise "The destination database appears to be production!  Is that really what you want?"
      end
    end

#  # For each view, filter definition and create on destination
#  con = Mysql.connect(DESTINATION_DATABASE[:host], DESTINATION_DATABASE[:username], 
#    DESTINATION_DATABASE[:password], DESTINATION_DATABASE[:database])
#  views_hash.each_pair do |view_name, view_def|
#    puts "Fixing #{view_name}"
#    fixed_view_def = fix_view_def(view_name, view_def, SECONDARY_DATABASES)
#    con.query(fixed_view_def)
#  end
#  con.close
  end
end
