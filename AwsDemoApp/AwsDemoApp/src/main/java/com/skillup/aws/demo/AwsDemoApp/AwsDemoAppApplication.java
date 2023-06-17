package com.skillup.aws.demo.AwsDemoApp;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.util.IOUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

//@SpringBootApplication
public class AwsDemoAppApplication {

	private static final AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

	public static void main(String[] args) {
//		SpringApplication.run(AwsDemoAppApplication.class, args);
		readDragonData();
	}

	private static void readDragonData(){
		String bucketName = getBucketName();
		String key = getKey();
		String query = getQuery();
		SelectObjectContentRequest request = generateJSONRequest(bucketName, key, query);
		String dragonData = queryS3(request);
		System.out.println(dragonData);
	}

	private static String queryS3(SelectObjectContentRequest request) {
		final AtomicBoolean isResultComplete = new AtomicBoolean(false);

		SelectObjectContentResult result = s3Client.selectObjectContent(request);

		InputStream resultInputStream = result.getPayload().getRecordsInputStream(
				new SelectObjectContentEventVisitor() {
					@Override
					public void visit(SelectObjectContentEvent.StatsEvent event) {
						System.out.println("Received stats, Bytes scanned: " + event.getDetails().getBytesScanned()
								+ " Bytes processed: " + event.getDetails().getBytesProcessed());
					}

					@Override
					public void visit(SelectObjectContentEvent.EndEvent event) {
						isResultComplete.set(true);
						System.out.println("Received end event. Result is complete");
					}
				}
		);
		String text = null;
		try {
			text = IOUtils.toString(resultInputStream);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return text;
	}

	private static SelectObjectContentRequest generateJSONRequest(String bucketName, String key, String query) {
		SelectObjectContentRequest request = new SelectObjectContentRequest();
		request.setBucketName(bucketName);
		request.setKey(key);
		request.setExpression(query);
		request.setExpressionType(ExpressionType.SQL);

		InputSerialization inputSerialization = new InputSerialization();
		inputSerialization.setJson(new JSONInput().withType("Document"));
		inputSerialization.setCompressionType(CompressionType.NONE);
		request.setInputSerialization(inputSerialization);

		OutputSerialization outputSerialization = new OutputSerialization();
		outputSerialization.setJson(new JSONOutput());
		request.setOutputSerialization(outputSerialization);

		return request;
	}

	private static String getBucketName() {
		GetParameterRequest bucketParameterRequest = new GetParameterRequest().withName("dragon_data_bucket_name").withWithDecryption(false);
		GetParameterResult bucketResult = ssm.getParameter(bucketParameterRequest);
		return bucketResult.getParameter().getValue();
	}

	private static String getKey() {
		GetParameterRequest keyParameterRequest = new GetParameterRequest().withName("dragon_data_file_name").withWithDecryption(false);
		GetParameterResult keyResult = ssm.getParameter(keyParameterRequest);
		return keyResult.getParameter().getValue();
	}

	private static String getQuery() {
		return "select * from s3object s";
	}
}
