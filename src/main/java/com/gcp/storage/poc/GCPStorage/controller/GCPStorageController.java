package com.gcp.storage.poc.GCPStorage.controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.storagetransfer.v1.proto.StorageTransferServiceClient;
import com.google.storagetransfer.v1.proto.TransferProto.CreateTransferJobRequest;
import com.google.storagetransfer.v1.proto.TransferTypes.AwsAccessKey;
import com.google.storagetransfer.v1.proto.TransferTypes.AwsS3Data;
import com.google.storagetransfer.v1.proto.TransferTypes.GcsData;
import com.google.storagetransfer.v1.proto.TransferTypes.Schedule;
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob;
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob.Status;
import com.google.storagetransfer.v1.proto.TransferTypes.TransferSpec;
import com.google.type.Date;
import com.google.type.TimeOfDay;
@RestController
@RequestMapping("/gcp")
public class GCPStorageController {
	
	

	private static final String UTF_8 = null;
	@Autowired
	private Storage storage;

	@GetMapping("list-all-buckets")
	public List<String> listAllBuckets(){
		List<String> li = new ArrayList<>(); 
		Storage storage = StorageOptions.newBuilder().setProjectId("migration-from-s3-poc").build().getService();
	    Page<Bucket> buckets = storage.list();

	    for (Bucket bucket : buckets.iterateAll()) {
	       li.add(bucket.getName());
	    }
	    return li;
	}
	
	@GetMapping("create-object-in-bucket")
	public String createObjectInBucket() throws IOException {
	    Storage storage = StorageOptions.newBuilder().setProjectId("migration-from-s3-poc").build().getService();
	    BlobId blobId = BlobId.of("first-bucket-poc", "Random.txt");
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
	    Storage.BlobTargetOption precondition;
	    storage.create(blobInfo, Files.readAllBytes(Paths.get("/Users/bbetikunti/Documents/Random.txt")));
		return "file uploaded to bucket - "+"first-bucket-poc";
	}
	@GetMapping("create-bucket")
	public String asl()  {

		Storage storage = StorageOptions.newBuilder().setProjectId("migration-from-s3-poc").build().getService();
	    Blob blob = storage.get("first-bucket-poc", "template888.txt");

	    ReadChannel readChannel = blob.reader();
        InputStream inputStream = Channels.newInputStream(readChannel);
        
        
       

//		Bucket bucket = storage.create(BucketInfo.of("baeldung-bucket-by-bhargav77"));
//		Page<Blob> blobs = bucket.list();
//		System.out.println("Buckets present in gcp");
//		for (Blob blob: blobs.getValues()) {
//		    System.out.println(blob.getName());
//		}
//		System.out.println("-------");
//		for (Blob blob: blobs.getValues()) {
//		    if ("baeldung-bucket-by-bhargav".equals(blob.getName())) {
//		        return new String(blob.getContent());
//		    }
//		}
//		String value = "Hello, World!";
//		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
//		Blob blob = bucket.create("my-first-blob", bytes);
		return "fetched";
	}
	@PostMapping("create-bucket")
	public String createBucket(@RequestParam String bucketName) {
		
		//specific target
	    //Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

		// Instantiates a client
	    Storage storage = StorageOptions.getDefaultInstance().getService();

	    System.out.println(bucketName);
	    // Creates the new bucket
	    Bucket bucket = storage.create(BucketInfo.of(bucketName));
	    System.out.println("asdbjahbd");
		return "Bucket is created"+ bucket.getName();
	}
	@GetMapping("")
	public String fileInBucket()  {
		Storage client = StorageOptions.getDefaultInstance().getService();
		   
	    for (Bucket bucket : client.list().iterateAll()) {
	        System.out.println("Bucket -- "+bucket.getName());
	      }
	    
	    Bucket bucket = client.get("migration-from-s3-poc");
	    bucket.getLabels().forEach((k, v) -> System.out.println((k + " = " + v))); // 'getLabels()' may return null
		return "fetched";
	}

	@GetMapping("/send-data")
	public String sendData() throws IOException {

		BlobId bid = BlobId.of("first-bucket-poc", "DemoFile.txt");
		BlobInfo binfo = BlobInfo.newBuilder(bid).build();
		File f = new File("/Users/bbetikunti/Documents/s3-storage/GCP-Storage-1/src/main/resources","DemoFile.txt");
		byte [] arr = Files.readAllBytes(Paths.get(f.toURI()));
		storage.create(binfo,arr);



		StringBuilder sb = new StringBuilder();

				try (ReadChannel channel = storage.reader("first-bucket-poc", "template888.txt")) {
					ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
					while (channel.read(bytes) > 0) {
						bytes.flip();
						String data = new String(bytes.array(), 0, bytes.limit());
						sb.append(data);
						bytes.clear();
					}
				}


		return sb.toString();
	}
	
	// Creates a one-off transfer job from Amazon S3 to Google Cloud Storage.
	@PostMapping("/migrate-from-aws-to-gcp")
	  public static void transferFromAws(@RequestBody CloudDetails cloudDetails)
	      throws IOException {

	    // Your Google Cloud Project ID
	    // String projectId = "your-project-id";

	    // A short description of this job
	    // String jobDescription = "Sample transfer job from S3 to GCS.";

	    // The name of the source AWS bucket to transfer data from
	    // String awsSourceBucket = "yourAwsSourceBucket";

	    // The name of the GCS bucket to transfer data to
	    // String gcsSinkBucket = "your-gcs-bucket";

	    // What day and time in UTC to start the transfer, expressed as an epoch date timestamp.
	    // If this is in the past relative to when the job is created, it will run the next day.
	    // long startDateTime =
	    //     new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2000-01-01 00:00:00").getTime();

	    // The ID used to access your AWS account. Should be accessed via environment variable.
	   // String awsAccessKeyId = System.getenv("AKIA3MMJHXN33PGGA2EC");
		String awsAccessKeyId = "AKIA3MMJHXN33PGGA2EC";
	    // The Secret Key used to access your AWS account. Should be accessed via environment variable.
	   // String awsSecretAccessKey = System.getenv("kPDnVC2ruOEX2T1i2gIKaXDhrpmX4xvhNNu1xW23");
		String awsSecretAccessKey = "kPDnVC2ruOEX2T1i2gIKaXDhrpmX4xvhNNu1xW23";
	    // Set up source and sink
	    TransferSpec transferSpec =
	        TransferSpec.newBuilder()
	            .setAwsS3DataSource(
	                AwsS3Data.newBuilder()
	                    .setBucketName(cloudDetails.getAwsSourceBucket())
	                    .setAwsAccessKey(
	                        AwsAccessKey.newBuilder()
	                            .setAccessKeyId(awsAccessKeyId)
	                            .setSecretAccessKey(awsSecretAccessKey)))
	            .setGcsDataSink(GcsData.newBuilder().setBucketName(cloudDetails.getGcsSinkBucket()))
	            .build();

	    // Note that this is a Date from the model class package, not a java.util.Date
	    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
		System.out.println( now.toString());
		now = now.plusSeconds(10);
		 Date startDate = 
			        Date.newBuilder()
			            .setYear(now.getYear())
			            .setMonth( now.getMonthValue())
			            .setDay( now.getDayOfMonth())
			            .build();
		 System.out.println(startDate+"---"+startDate.toString());
		  TimeOfDay startTime =
			        TimeOfDay.newBuilder()
			            .setHours(now.getHour())
			            .setMinutes(now.getMinute())
			            .setSeconds(now.getSecond())
			            .build();
		  System.out.println(startTime+"---"+startTime.toString());
	    Schedule schedule =
	        Schedule.newBuilder()
	            .setScheduleStartDate(startDate)
	            .setScheduleEndDate(startDate)
	            .setStartTimeOfDay(startTime)
	            .build();

	    // Set up the transfer job
	    TransferJob transferJob =
	        TransferJob.newBuilder()
	            .setDescription(cloudDetails.getJobDescription())
	            .setProjectId(cloudDetails.getProjectId())
	            .setTransferSpec(transferSpec)
	            .setSchedule(schedule)
	            .setStatus(Status.ENABLED)
	            .build();

	    // Create a Transfer Service client
	    StorageTransferServiceClient storageTransfer = StorageTransferServiceClient.create();

	    // Create the transfer job
	    TransferJob response =
	        storageTransfer.createTransferJob(
	            CreateTransferJobRequest.newBuilder().setTransferJob(transferJob).build());

	    System.out.println("Created transfer job from AWS to GCS:");
	    System.out.println(response.toString());
	  }
	
}