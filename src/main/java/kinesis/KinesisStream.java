package kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

/**
 * Class to represent a Kinesis Stream.  Includes methods to 
 * start the stream , wait for the stream to become active and to 
 * put String records onto the stream. We do not get records from
 * the stream as it is recommended to use the Amazon Kinesis Client
 * Library.
 * @author T Sheppard
 *
 */
public class KinesisStream {

	/**
	 * Constructor to set internal member variables.
	 * @param client - An AmazonKinesisClient, should already have stream location info 
	 * @param streamName - the name of the stream
	 * @param numShards - the number of shards in the stream
	 */
	public KinesisStream(AmazonKinesisClient client, String streamName, int numShards){
		this.client = client;
		this.streamName = streamName;
		this.numShards = numShards;
	}
	
	private AmazonKinesisClient client;
	private String streamName;
	private int numShards;

	/**
	 * Creates a new stream using the variables passed int he constructor.
	 * @throws ResourceNotFoundException
	 * @throws LimitExceededException
	 * @throws InvalidArgumentException
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 */
	public void startStream() throws ResourceNotFoundException, LimitExceededException, InvalidArgumentException, AmazonClientException, AmazonServiceException {
		
		CreateStreamRequest streamRequest = new CreateStreamRequest();
		streamRequest.setStreamName(this.streamName);
		streamRequest.setShardCount(numShards);
		
		this.client.createStream(streamRequest); 
		//throws LimitExceededException, ResourceInUseException, InvalidArgumentException, AmazonClientException, AmazonServiceException
	}
	
	
	/**
	 * Checks whether the stream has become active.  Checks 5 times in 100ms intervals.
	 * @return boolean indicating whether stream is now active
	 * @throws InterruptedException
	 * @throws ResourceNotFoundException
	 * @throws LimitExceededException
	 */
	public boolean checkStreamActive() throws InterruptedException, ResourceNotFoundException, LimitExceededException {  
		
		//Wait for the stream to become active
		DescribeStreamRequest descStreamReq = new DescribeStreamRequest();
		descStreamReq.setStreamName(streamName);
		String streamStatus = "DEFAULT";
				
		streamStatus = getStreamStatus(descStreamReq, 5); 
		//throws InterruptedException, ResourceNotFoundException, LimitExceededException 
		
		if(streamStatus.equals("ACTIVE"))
			return true;
		return false;				
	}
	
	/**
	 * Get the status of the stream.
	 * @param descStreamReq - a describe stream request object
	 * @param getStreamAttempts - number of times to attempt to get ACTIVE result 
	 * @return String containing the status of the stream
	 * @throws InterruptedException
	 * @throws ResourceNotFoundException
	 * @throws LimitExceededException
	 */
	private String getStreamStatus(DescribeStreamRequest descStreamReq, int getStreamAttempts) throws InterruptedException, ResourceNotFoundException, LimitExceededException {
		
		DescribeStreamResult descStremRes = client.describeStream(descStreamReq); 
		//throws ResourceNotFoundException, LimitExceededException 
		
		String status = descStremRes.getStreamDescription().getStreamStatus();
		
		if(!status.equals("ACTIVE") && getStreamAttempts > 0) {
			Thread.sleep(100);//throws InterruptedException
			status = getStreamStatus(descStreamReq, getStreamAttempts-1);
		}
		
		return status;
	}
	
	
	/**
	 * Add the data in the list to the kinesis stream via the putRecords method.
	 * 
	 * @param dataList
	 * @throws RuntimeException
	 * @throws ProvisionedThroughputException
	 * @throws InvalidArgumentException
	 * @throws ResourceNotFoundException
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 */
	public <T> List<T> putMultipleData(List<T> dataList) 
			throws RuntimeException, ProvisionedThroughputException, InvalidArgumentException,  ResourceNotFoundException, 
			AmazonClientException, AmazonServiceException {
		
		PutRecordsRequest putRecsReq = new PutRecordsRequest();
		putRecsReq.setStreamName(streamName);
			
		List<PutRecordsRequestEntry> entryList = new ArrayList<PutRecordsRequestEntry>();
			
		for(T item : dataList) {
			PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
			entry.setData(ByteBuffer.wrap((item.toString()).getBytes()));//Should get better way to serialise data
			entryList.add(entry);
		}
			
		putRecsReq.setRecords(entryList);
		PutRecordsResult putRecsRes = client.putRecords(putRecsReq);
			
		//Get the failed entries
		List<T> failed = new ArrayList<T>();
		int index = 0;
		if(putRecsRes.getFailedRecordCount()!=0) {
			List<PutRecordsResultEntry> results  = putRecsRes.getRecords();
			for(PutRecordsResultEntry entry : results) {
				if(!entry.getErrorCode().equals("")) 
					failed.add(dataList.get(index));
				index++;
			}
		}
		return failed;
	}
}
