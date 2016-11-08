This is a simple tool to go through and collect SSTable Metadata on all SSTables in a specific Cassandra table. It will create a folder that contains the raw output for all the SSTableMetadata ouputs as well as generate a CSV file that aggregates that information

#Usage
python SSTableParser.py [cassandra_data_dir] [keyspace] [table] [is_dse=true/false] [optional, path_to_sstablemetadata]

python SSTableParser.py true /var/lib/cassandra/data test_keyspace test_table true
python SSTableParser.py true /var/lib/cassandra/data test_keyspace test_table false /path/to/sstablemetadata