def readS3(topics):
    for item in topics:
        # Define S3 partition path
        partition_path = f"s3a://user-57e94de2a910-bucket/topics/57e94de2a910{item}/partition=0/"
        
        # Read all JSON files in the partition
        df = spark.read.format("json").load(partition_path + "*.json")
        
        # Save directly as a Delta table in Databricks
        table_name = f"57e94de2a910_{item[1:]}"  # e.g., "57e94de2a910_pin"
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Data from {partition_path} saved as table {table_name}")

# Topic List
topics = [".pin", ".geo", ".user"]

# Run the function
readS3(topics)