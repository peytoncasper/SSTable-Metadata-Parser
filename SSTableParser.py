import subprocess, os, re, sys, datetime

def is_sstable(sstable_path):
    # Check for whether or not sstable_path is a valid SSTable directory
    for file in os.listdir(sstable_path):
        if re.match("mc-[0-9]+-big-Data.db", file):
            return sstable_path + "/" + file

    return False
def process_metadata(output, name):
    lines = output.split("\n")

    size_start = 0
    size_end = 0
    min_timestamp = 0
    max_timestamp = 0
    estimated_cardinality = 0
    for i in range(0, len(lines)):
        line = lines[i]
        # Find the start of byte counts
        if re.match("Count[ ]+Row Size[ ]+Cell Count", line):
            size_start = i + 1
        # Find the end of byte counts
        if re.match("Estimated cardinality:", line):
            size_end = i
            estimated_cardinality = i + 1
        # Find timestamp rows
        if re.match("Minimum timestamp:", line):
            min_timestamp = line.split(" ")[2]
            max_timestamp = lines[i + 1].split(" ")[2]

    # Sum all of the byte rows
    sum_bytes_row = 0
    for i in range(size_start, size_end):
        count,row,cell = re.sub('\s+', ',', lines[i]).split(",")
        if row != '':
            sum_bytes_row += int(row)


    return [name,
            sum_bytes_row,
            min_timestamp,
            datetime.datetime.fromtimestamp(float(min_timestamp)/1e9).strftime('%Y-%m-%d %H:%M:%S'),
            max_timestamp,
            datetime.datetime.fromtimestamp(float(max_timestamp)/1e9).strftime('%Y-%m-%d %H:%M:%S'),
            lines[1].split(" ")[1],
            lines[2].split(" ")[4],
            lines[5].split(" ")[5],
            lines[6].split(" ")[2],
            lines[7].split(" ")[3],
            lines[8].split(" ")[2],
            estimated_cardinality]
    # return [SSTableName,
    #         Sum of SSTable Row Bytes
    #         Minimum Timestamp
    #         Minimum Timestamp converted
    #         Maximum Timestamp
    #         Maximum Timestamp converted
    #         Partitioner
    #         Bloom Filer Chance
    #         SSTable Max Local Deletion Time
    #         Compression Ratio
    #         Estimated Droppable Tombstones
    #         SSTable Level
    #         Repaired At

def main():
    # Check for the appropriate number of args
    if len(sys.argv) < 5:
        print "Please include the following arguments [cassandra_data_dir] [keyspace] [table] [is_dse=true/false] [optional, path_to_sstablemetadata]"
        sys.exit(1)

    # Store the command line args
    path = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    dse = sys.argv[4]
    if dse == 'false':
        if len(sys.argv) >= 6:
            path_to_sstablemetadata = sys.argv[5]
        else:
            print "Please include the following arguments [cassandra_data_dir] [keyspace] [table] [is_dse=true/false] [optional, path_to_sstablemetadata]"
            sys.exit(1)
    # Ensure the directory we are going to store metadata at exists, else create it
    metadatadir = keyspace + "-" + table
    if not os.path.exists(metadatadir):
        os.makedirs(metadatadir)

    base_data_dir = path + "/" + keyspace + "/"
    csv = []

    # For each data directory in the specified keyspace
    for x in os.listdir(base_data_dir):
        # Check to see if the data directory pulled contains an SSTable
        sstable = is_sstable(base_data_dir + x)
        if re.match(table + "-*",x) and sstable:
            # Valid SSTable, run dse sstablemetadata
            command = ""
            if dse == 'true':
                command = "dse sstablemetadata " + sstable
            else:
                command = path_to_sstablemetadata + " " + sstable

            process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            # If an error hasnt been received, write the output into our metadata directory locally
            if error == None:
                file = open(metadatadir + "/" + x, "w")
                file.write(output)
                file.close()
                #Process Metadata and store the results to output to CSV later
                csv.append(process_metadata(output,x))
            else:
                print "ERROR     " + error

    csv_file = open(metadatadir + "/" + "report.csv", "w")
    csv_file.writelines("Name,Size,Minimum Timestamp,Minimum Date,Maximum Timestamp,Maximum Date,"
                        "Partitioner,Bloom Filter Chance,SSTable Max Local Deletion Time,Compression Ratio"
                        ",Estimated Droppable Tombstones,SSTable Level,Repaired At\n")

    for sstable in csv:
        for data in sstable:
            csv_file.write(str(data) + ",")

        csv_file.write("\n")
    csv_file.close()
if __name__ == "__main__":
    main()