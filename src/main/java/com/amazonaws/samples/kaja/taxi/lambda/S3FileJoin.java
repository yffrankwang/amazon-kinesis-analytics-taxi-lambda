package com.amazonaws.samples.kaja.taxi.lambda;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileJoin implements RequestHandler<Map<String, Object>, String> {
	private static final String bucketRegion = "us-east-1";
	private static final String bucketName = "tlc-stack-artifactbucket-wwus3ytvirwq";
	
	@Override
	public String handleRequest(Map<String, Object> input, Context context) {
		final LambdaLogger LOG = context.getLogger();

		final S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();

		listS3Objects(s3);

		return "200 OK";
	}
	
	public static List<S3Object> listS3Objects(S3Client s3) {
		String prefix = "kinesis-output/20211213";
		
		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
		Iterator<S3Object> s3ObjIter = s3.listObjectsV2Paginator(request).contents().iterator();
		
		List<S3Object> s3objs = new ArrayList<S3Object>();
		while (s3ObjIter.hasNext()) {
			s3objs.add(s3ObjIter.next());
		}

		s3objs.sort(new Comparator<S3Object>() {
			@Override
			public int compare(S3Object o1, S3Object o2) {
				String[] ss1 = StringUtils.split(o1.key(), '-');
				String[] ss2 = StringUtils.split(o1.key(), '-');
				
				if (ss1.length != ss2.length) {
					return ss1.length - ss2.length;
				}
				
				int c;
				for (int i = 0; i < ss1.length; i++) {
					String s1 = ss1[i];
					String s2 = ss2[i];
					
					if (NumberUtils.isDigits(s1) && NumberUtils.isDigits(s2)) {
						c = Integer.parseInt(s1) - Integer.parseInt(s2);
					} else {
						c = s1.compareTo(s2);
					}
					
					if (c != 0) {
						return c;
					}
				}
				return 0;
			}
		});
		
		return s3objs;
	}
	
	public static void main(String[] args) throws Exception {
		final S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();
		
		List<S3Object> s3objs = listS3Objects(s3);
		
		for (S3Object o : s3objs) {
			System.out.println(String.format("%20s  %d", o.key(), o.size()));
		}
	}
}

