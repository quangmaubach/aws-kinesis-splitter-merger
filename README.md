# kinesis-splitter-merger
Simple tool to split a specific shard of the given AWS Kinesis stream in two.

You can also list all the shards of a stream using this tool

##Run

```bash
$ mvn clean install

# To split a specific shard
$ java -cp target/kinesis-splitter-merger-1.0.jar awsutils.KinesisSplitter <stream-name> <aws-access-key> <aws-secret-key> <shard_id_to_split>

# To list all the shards of a stream, together with their hash ranges
java -cp target/kinesis-splitter-merger-1.0.jar awsutils.KinesisMerger <stream-name> <aws-access-key> <aws-secret-key>

# To merge 2 shards of a stream
java -cp target/kinesis-splitter-merger-1.0.jar awsutils.KinesisMerger <stream-name> <aws-access-key> <aws-secret-key> <shard_id_to_merge_1> <shard_id_to_merge_2>
```
