module Taps
  module Utils
    def format_data(data, opts={})
      return {} if data.size == 0
      string_columns = opts[:string_columns] || []
      schema = opts[:schema] || []
      table  = opts[:table]

      max_lengths = schema.inject({}) do |hash, (column, meta)|
        if meta[:db_type] =~ /^varchar\((\d+)\)/
          hash.update(column => $1.to_i)
        end
        hash
      end

      header = data[0].keys
      only_data = data.collect do |row|
        row = blobs_to_string(row, string_columns)
        row.each do |column, data|
          if data.to_s.length > (max_lengths[column] || data.to_s.length)
            raise Taps::InvalidData.new(<<-ERROR)
  Detected data that exceeds the length limitation of its column. This is
  generally due to the fact that SQLite does not enforce length restrictions.

  Table  : #{table}
  Column : #{column}
  Type   : #{schema.detect{|s| s.first == column}.last[:db_type]}
  Data   : #{data}
            ERROR
          end
        end
        header.collect { |h| row[h] }
      end
      { :header => header, :data => only_data }
    end

    # mysql text and blobs fields are handled the same way internally
    # this is not true for other databases so we must check if the field is
    # actually text and manually convert it back to a string
    def incorrect_blobs(db, table)
      return [] if (db.url =~ /mysql:\/\//).nil?

      columns = []
      db.schema(table).each do |data|
        column, cdata = data
        columns << column if cdata[:db_type] =~ /text/
      end
      columns
    end

    def blobs_to_string(row, columns)
      return row if columns.size == 0
      columns.each do |c|
        row[c] = row[c].to_s if row[c].kind_of?(Sequel::SQL::Blob)
      end
      row
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


  end
end