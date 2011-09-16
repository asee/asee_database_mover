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
      @dump = cnf['defaults']['dump']
      @src_cnf = {
        :host => cnf[prj][src]['host'],
        :database => "#{@prj}_#{@src}",
        :username => cnf[prj][src]['username'],
        :password => cnf[prj][src]['password']  
      }
      @tgt_cnf = {
        :host => cnf[prj][src]['host'],
        :database => "#{@prj}_#{@tgt}",
        :username => cnf[prj][src]['username'],
        :password => cnf[prj][src]['password']  
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
    end

    # Fixes a "create view" statement to the right format for the destination db.
    def fix_view_def(view_name, view_def, secondary_databases)
      fixed_view_def = view_def.gsub(/\A.* AS select /, "create or replace view #{view_name} as select ")
      secondary_databases.each_pair do |from, to|
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
        db = "#{dep_db}_#{src}"
        dump_db({},db)
      end

    end

    def load_db(db_hash, source_db_name)
      command = "#{MYSQLADMIN} #{db_command_options(db_hash, false)} create #{db_hash[:database]}"
      puts command
      `#{command}`
      
      command = "#{MYSQL} #{db_command_options(db_hash)} < mysqldumps/#{source_db_name}.sql"
      puts command
      `#{command}`
    end

    def db_command_options(db_hash, include_database=true)
      include_password = !(db_hash[:password].nil? || db_hash[:password] == "")
      p = include_password ? "-p#{db_hash[:password]}" : ""
      d = include_database ? db_hash[:database] : ""
      "-h #{db_hash[:host]} -u #{db_hash[:username]} #{p} #{d}"
    end


    def perform_sanity_check
      if @tgt_cnf[:database] =~ /_production/
        raise "The destination database appears to be production!  Is that really what you want?"
      end
    end

#  # dump secondary databases
#  SECONDARY_DATABASES.each_key do |db|
#    puts "Dumping #{db}"
#    dump_db(SOURCE_DATABASE.merge(:database => db))
#  end
#
#  # restore primary and secondary databases
#  load_db(DESTINATION_DATABASE, SOURCE_DATABASE[:database])
#  SECONDARY_DATABASES.each_pair do |source, dest|
#    puts "Restoring #{source} to #{dest}"
#    load_db(DESTINATION_DATABASE.merge(:database => dest), source)
#  end
#
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
