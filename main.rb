require './db_copier.rb'

#src_db_url = 'sqlite://src.db'
src_db_hash = {
  adapter: 'mysql2',
  host: 'localhost',
  database: 'islamopedia',
  user: 'islamopedia',
  password: 'I$l@m0ped1a' 
}

dest_db_hash1 = {
  adapter: 'mysql2',
  host: 'localhost',
  database: 'islamopedia_copy',
  user: 'root',
  password: ''   
}

dest_db_hash2 = {
  adapter: 'sqlite',
  database: 'islamopedia_copy.db'
}

DbCopier.new(src_db_hash, dest_db_hash2).copy(
#[
#  :books,
#  :videos
#],
#:skip_schema => 1, :truncate_tables => 1
#:skip_data => 1
)
#DbCopier.new(src_db_hash, dest_db_hash2).copy
