package awsutils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.util.StringUtils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

import lombok.extern.java.Log;

/**
 * Simple tool to merge shards of the given AWS Kinesis stream.
 */
@Log
public class KinesisMerger {

    public static void main(String[] args) {
        //System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s %5$s%6$s%n");
        String streamName = args[0];
        String awsAccessKey = null;
        String awsSecretKey = null;
        String firstShardToMerge = null;
        String secondShardToMerge = null;

        if (args.length >= 3) {
            awsAccessKey = args[1];
            awsSecretKey = args[2];
        }

        if (args.length >= 5) {
            firstShardToMerge = args[3];
            secondShardToMerge = args[4];
        }

        if (firstShardToMerge == null) {
            log.info("No ShardToMerge parameters provided. Will print shards info");
        } else if ("".equalsIgnoreCase(firstShardToMerge) || "".equalsIgnoreCase(secondShardToMerge)) {
            log.warning("Invalid ShardToMerge parameters provided.");
            System.exit(-1);
        } else {
            log.log(Level.INFO, "Will merge shard {0} and {1}", new String[] {firstShardToMerge, secondShardToMerge});
        }

        try {
            new KinesisMerger().split(streamName, awsAccessKey, awsSecretKey, firstShardToMerge, secondShardToMerge);
        } catch (final InterruptedException | AmazonClientException ex) {
            log.log(Level.SEVERE, "Error while merging the stream: " + streamName, ex);
        }
    }

    public void split(final String streamName, final String awsAccessKey, final String awsSecretKey, String firstShardToMerge, String secondShardToMerge)
            throws InterruptedException {

        AWSCredentialsProvider creds = createAwsCredentialsProvider(awsAccessKey, awsSecretKey);
        AmazonKinesisClient client = new AmazonKinesisClient(creds);

        // Describes the stream to get the information about each shard
        List<Shard> shards = client.describeStream(streamName).getStreamDescription().getShards();

        if (firstShardToMerge == null && secondShardToMerge == null) {
            shards.stream()
                    .sorted((x, y) -> new BigInteger(x.getHashKeyRange().getStartingHashKey()).compareTo(new BigInteger(y.getHashKeyRange().getStartingHashKey())))
                    .filter(shard -> shard.getSequenceNumberRange().getEndingSequenceNumber() == null) // Shards that are OPEN
                    .forEach(shard ->
                            log.log(Level.INFO, "ShardId: {0}, StartKey: {1} EndKey: {2}",
                                    new String[]{shard.getShardId(),
                                            shard.getHashKeyRange().getStartingHashKey(),
                                            shard.getHashKeyRange().getEndingHashKey()})
                    );
        } else {
            boolean foundFirstShard = false;
            boolean foundSecondShard = false;
            for (Shard shard: shards) {
                if (shard.getShardId().equals(firstShardToMerge)) {
                    foundFirstShard = true;
                    continue;
                }

                if (shard.getShardId().equals(secondShardToMerge)) {
                    foundSecondShard = true;
                }
            }

            if (foundFirstShard && foundSecondShard) {
                MergeShardsRequest mergeShardsRequest = new MergeShardsRequest();
                mergeShardsRequest.setStreamName(streamName);
                mergeShardsRequest.setShardToMerge(firstShardToMerge);
                mergeShardsRequest.setAdjacentShardToMerge(secondShardToMerge);
                client.mergeShards(mergeShardsRequest);
            } else {
                log.log(Level.WARNING, "ShardId(s) not found in Kinesis stream. All available shards: " + Arrays.toString(
                        shards.stream().map(shard -> shard.getShardId()).collect(Collectors.toList()).toArray()
                ));
            }
        }
    }

    public static AWSCredentialsProvider createAwsCredentialsProvider(final String accessKey, final String secretKey) {
        if (!StringUtils.isNullOrEmpty(accessKey) && StringUtils.isNullOrEmpty(secretKey)) {
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKey, secretKey);
                }

                @Override
                public void refresh() {
                }
            };
        }
        return new DefaultAWSCredentialsProviderChain();
    }
}
