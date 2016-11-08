SSTable-Metadata-Parse is a simple tool that takes in a Cassandra table and runs through each table aggregating the metadata into a folder
and generating a single CSV that contains all of the metadata information.

#Notes
This tool needs access to your Cassandra data directory which means it needs the correct permissions.

#Usage
python SSTableParser.py [cassandra_data_dir] [keyspace] [table] [is_dse=true/false] [optional, path_to_sstablemetadata]

* python SSTableParser.py true /var/lib/cassandra/data test_keyspace test_table true
* python SSTableParser.py true /var/lib/cassandra/data test_keyspace test_table false /path/to/sstablemetadata