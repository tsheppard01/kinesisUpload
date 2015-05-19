package kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
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
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class Kinesis {
	
	int main() {
		
		//Kinesis client variables
		String endpoint = " "; //aws kinesi url see http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
		String regionId = " "; //aws region name see http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
		String serviceName = "kinesis";
		
		//Stream parameters
		String streamName = "testKinesisStream";
		int numShards = 1;
		
		//To be initialised
		//AWSCredentials awsCred;
		
		//Create a new KinesisAccessClient and set thepointers to AWS
		AmazonKinesisClient client =  new AmazonKinesisClient();//awsCred);
		client.setEndpoint(endpoint, serviceName, regionId);
		
		//Create a KinesisStream object
		KinesisStream kStream = new KinesisStream(client, streamName, numShards);

		//Create the actual stream.  This function blocks until stream is active or timesout
		kStream.startStream();
		
		//Add data to the stream	
		List<String> dataList = Arraylist<String>();
		for(int i=0;i<25;i++) {
			int tmp = i*4;
			toAdd.dataList(new Integer(tmp).toString());
		}
	
		if(kStream.checkStreamActive()) {
			List<String> failed;
			do
				failed = kStream.putMultipleData(dataList);
			 while(failed.size()!=0); 
		} else {
			//Should try and wait longer for stream to become active
			throw new RuntimeException("Stream " + streamName + " status is not active: " + streamStatus + ".");
		}
	}
	
}
