package com.amazonaws.samples.kaja.taxi.lambda;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3FileJoin implements RequestHandler<Map<String, Object>, String> {
	private static final String bucketRegion = "us-east-1";
	private static final String bucketName = "tlc-stack-artifactbucket-wwus3ytvirwq";
	private static final FastDateFormat DATEFMT = FastDateFormat.getInstance("yyyyMMdd");

	@Override
	public String handleRequest(Map<String, Object> input, Context context) {
		final LambdaLogger LOG = context.getLogger();

		final S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();

		List<S3Object> s3objs = listS3Objects(s3);

		DecimalFormat df = new DecimalFormat("###,###");
		long sumSize = 0;
		for (S3Object o : s3objs) {
			sumSize += o.size();
			LOG.log(String.format("%50s  %-20s", o.key(), df.format(o.size())));
		}

		LOG.log("-------------------------------");
		LOG.log("  Total files: " + s3objs.size());
		LOG.log("  Total size : " + df.format(sumSize));

		String key = "lambda-output/" + DATEFMT.format(System.currentTimeMillis()) + ".csv";

		System.out.println("-------------------------------");
		LOG.log("Start join S3 Objects and save to " + key);

		try {
			S3JoinInputStream sjis = new S3JoinInputStream(s3, s3objs);

			PutObjectRequest objectRequest = PutObjectRequest.builder()
				.bucket(bucketName)
				.key(key)
				.build();

			s3.putObject(objectRequest, RequestBody.fromInputStream(sjis, sumSize));
		} catch (Exception e) {
			LOG.log(ExceptionUtils.getStackTrace(e));
			return "500 ERROR";
		}

		LOG.log("-------------------------------");
		LOG.log("Complete save to " + key);

		return "200 OK";
	}

	public static List<S3Object> listS3Objects(S3Client s3) {
		long yesterday = System.currentTimeMillis() - DateUtils.MILLIS_PER_DAY;

		String prefix = "kinesis-output/" + DATEFMT.format(yesterday);

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
		Iterator<S3Object> s3ObjIter = s3.listObjectsV2Paginator(request).contents().iterator();

		List<S3Object> s3objs = new ArrayList<S3Object>();
		while (s3ObjIter.hasNext()) {
			s3objs.add(s3ObjIter.next());
		}

		s3objs.sort(new Comparator<S3Object>() {
			/**
			 * file name: part-{PartNo}-{SpliltNo}
			 * compapre order:
			 * 1. {SplitNo}
			 * 2. {PartNo}
			 * 
			 * example: part-0-1 < part-0-10
			 */
			@Override
			public int compare(S3Object o1, S3Object o2) {
				String p1 = StringUtils.substringBeforeLast(o1.key(), "/");
				String p2 = StringUtils.substringBeforeLast(o2.key(), "/");
				String n1 = StringUtils.substringBefore(StringUtils.substringAfterLast(o1.key(), '/'), '.');
				String n2 = StringUtils.substringBefore(StringUtils.substringAfterLast(o2.key(), '/'), '.');

				int c = p1.compareTo(p2);
				if (c != 0) {
					return c;
				}

				String[] ss1 = StringUtils.split(n1, '-');
				String[] ss2 = StringUtils.split(n2, '-');

				if (ss1.length != ss2.length) {
					return ss1.length - ss2.length;
				}

				for (int i = ss1.length - 1; i >= 0; i--) {
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

	public static class S3JoinInputStream extends InputStream {
		private LambdaLogger LOG;
		private InputStream istream;
		private final S3Client s3;
		private final Iterator<S3Object> s3Objects;

		public S3JoinInputStream(S3Client s3, List<S3Object> s3objs) throws IOException {
			this.s3 = s3;
			this.s3Objects = s3objs.iterator();
			nextS3Object();
		}

		@Override
		public int read() throws IOException {
			while (istream != null) {
				int b = istream.read();
				if (b != -1) {
					return b;
				}
				if (!nextS3Object()) {
					return -1;
				}
			}

			return -1;
		}

		private void log(String msg) {
			if (LOG != null) {
				LOG.log(msg);
			} else {
				System.out.println(msg);
			}
		}

		private boolean nextS3Object() throws IOException {
			while (s3Objects.hasNext()) {
				// if another object has been previously read, close it before opening another one
				safeClose();

				// try to open the next S3 object
				S3Object s3Object = s3Objects.next();

				log("---------------------------------------------------");
				log("reading object s3://" + bucketName + "/" + s3Object.key());
				GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
				istream = new BufferedInputStream(s3.getObject(request));
				return true;
			}

			log("no next s3 object");
			return false;
		}

		public void safeClose() {
			if (istream != null) {
				try {
					istream.close();
				} catch (IOException e) {
					log("failed to close object: " + e.getMessage());
				}

				istream = null;
			}
		}

		public void close() throws IOException {
			safeClose();
		}

	}

	public static void main(String[] args) throws Exception {
		final S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();

		List<S3Object> s3objs = listS3Objects(s3);

		DecimalFormat df = new DecimalFormat("###,###");
		long sumSize = 0;
		for (S3Object o : s3objs) {
			sumSize += o.size();
			System.out.println(String.format("%-50s  %20s", o.key(), df.format(o.size())));
		}

		System.out.println("-------------------------------");
		System.out.println("  Total files: " + s3objs.size());
		System.out.println("  Total size : " + df.format(sumSize));

		String key = "lambda-output/" + DATEFMT.format(System.currentTimeMillis()) + ".csv";

		System.out.println();
		System.out.println("-------------------------------");
		System.out.println("Start join S3 Objects and save to " + key);

		S3JoinInputStream sjis = new S3JoinInputStream(s3, s3objs);

		PutObjectRequest objectRequest = PutObjectRequest.builder()
			.bucket(bucketName)
			.key(key)
			.build();

		s3.putObject(objectRequest, RequestBody.fromInputStream(sjis, sumSize));

		System.out.println("-------------------------------");
		System.out.println("Complete save to " + key);
	}
}
