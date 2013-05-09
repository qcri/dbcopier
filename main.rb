require './db_copier.rb'

src_db_hash = {
  adapter: 'postgres',
  host: 'localhost',
  database: 'medscraper',
  user: 'medscraper',
  password: 'medsecret' 
}

dest_db_hash = {
  adapter: 'mysql2',
  #host: 'localhost',
  host: '127.0.0.1',
  port: 3307,
  database: 'medscraper-sranalysis',
  user: 'root',
  password: 'QCRI-DA/$ecure'
}

dest_db_aws = {
  adapter: 'postgres',
  host: 'localhost',
  database: 'medscraper',
  user: 'medscraper',
  password: 'medsecret',
  port: 5433
}

DbCopier.new(src_db_hash, dest_db_aws).copy(
[
  :paragraphs
],
:continue => 1
#:skip_schema => 1,
#:truncate_tables => 1,
#:skip_data => 1
)
#DbCopier.new(src_db_hash, dest_db_hash2).copy
