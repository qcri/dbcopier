require 'sequel'
require 'sequel/extensions/migration'
require 'sequel/extensions/schema_dumper'
require 'progress_bar'
require './taps_utils'

class DbCopier
  include Taps::Utils

  def initialize(src_db_hash, dest_db_hash)
    @src_db = verify_db_url(src_db_hash)
    @dest_db = verify_db_url(dest_db_hash)
    @fetch_limit = 1000
  end

  def verify_db_url(db_hash)
    db_hash[:default_schema] = db_hash[:schema] || 'public' if db_hash[:adapter] == 'postgres'
    db = Sequel.connect(db_hash)
    db.tables
    return db
  rescue Object => e
    raise "Failed to connect to database at #{db_hash}:\n  #{e.class} -> #{e}"
  end

  def info
    puts "Source:"
    puts @src_db.tables
    puts "Dest:"
    puts @dest_db.tables
  end

  def copy(tables = nil, opt = {})
    # TODO: data filters
    t1 = Time.now
    grand_total = 0

    tables = @src_db.tables unless tables

    puts "Processing #{tables.length} tables(s): #{tables.join(', ')}"

    copy_schema(tables) unless opt[:skip_schema] || opt[:continue]
    grand_total = copy_data(tables, opt) unless opt[:skip_data]
    copy_indices(tables)
    reset_sequences(tables)

    tdiff = Time.now - t1
    tdiff_min = (tdiff/60).floor
    tdiff_sec = tdiff - tdiff_min * 60
    puts "Finished copying database (#{grand_total} records) in #{tdiff_min} minutes, #{tdiff_sec} seconds (#{grand_total/tdiff} r/s)"
  end

  protected

  def copy_schema(tables)
    copy_operation(tables, "Copying schema:") do |table|
      up = @src_db.dump_table_schema(table.identifier, :indexes => false)
      down = "drop_table('#{table}') if @db.table_exists?('#{table}')"
      mig = migration(table, up, down)
      load(mig)
    end
  end

  def copy_data(tables, opt)
    grand_total = 0
    puts "Copying #{tables.length} table(s) data:"
    tables.each do |table|
      puts "Table #{table}:"
      total = @src_db[table.identifier].count
      grand_total += total
      pb = ProgressBar.new(total)
      offset = opt[:continue] ? @dest_db[table.identifier].count : 0
      pb.increment! offset if offset > 0
      @dest_db[table.identifier].truncate if opt[:truncate_tables] && offset == 0
      while 1
        rows = fetch_rows(@src_db, table, offset)
        break unless rows[:data]
        @dest_db[table.identifier].import(rows[:header], rows[:data]) 
        offset += @fetch_limit
        pb.increment!(rows[:data].length)
      end
    end
    grand_total
  end

  def copy_indices(tables)
    copy_operation(tables, "Copying indices:") do |table|
      up = @src_db.send(:dump_table_indexes, table, :add_index)
      mig = migration(table, up, "")
      load(mig)
    end
  end

  def reset_sequences(tables)
    copy_operation(tables, "Resetting table sequences:") do |table|
      @dest_db.reset_primary_key_sequence(table) if @dest_db.respond_to?(:reset_primary_key_sequence)
    end
  end

  def copy_operation(tables, message)
    puts message
    pb = ProgressBar.new(tables.length)
    tables.each do |table|
      yield table
      pb.increment!
    end
  end

  def migration(table, up, down)
    <<END_MIG
Class.new(Sequel::Migration) do
  def up
    #{up}
  end

  def down
    #{down}
  end
end
END_MIG
  end

  def load(migration)
    klass = eval(migration)
    klass.apply(@dest_db, :down)
    klass.apply(@dest_db, :up)
  end

  def fetch_rows(db, table, offset)
    table_name = table.identifier
    ds = db[table].order(*order_by(db, table)).limit(@fetch_limit, offset)
    format_data(ds.all,
      :string_columns => incorrect_blobs(db, table_name),
      :schema => db.schema(table_name),
      :table  => table_name
    )
  end

  def primary_key(db, table)
    db.schema(table).select { |c| c[1][:primary_key] }.map { |c| c[0] }
  end

  def order_by(db, table)
    pkey = primary_key(db, table)
    if pkey
      pkey.kind_of?(Array) ? pkey : [pkey.to_sym]
    else
      table = table.to_sym.identifier unless table.kind_of?(Sequel::SQL::Identifier)
      db[table].columns
    end
  end

  def next_offset(db, table)
    db[table.identifier].count
    
  end

end
