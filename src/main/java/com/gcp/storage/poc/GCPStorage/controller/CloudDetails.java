package com.gcp.storage.poc.GCPStorage.controller;

public class CloudDetails {
	String projectId;
    String jobDescription;
    String awsSourceBucket;
    String gcsSinkBucket;
	public String getProjectId() {
		return projectId;
	}
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	public String getJobDescription() {
		return jobDescription;
	}
	public void setJobDescription(String jobDescription) {
		this.jobDescription = jobDescription;
	}
	public String getAwsSourceBucket() {
		return awsSourceBucket;
	}
	public void setAwsSourceBucket(String awsSourceBucket) {
		this.awsSourceBucket = awsSourceBucket;
	}
	public String getGcsSinkBucket() {
		return gcsSinkBucket;
	}
	public void setGcsSinkBucket(String gcsSinkBucket) {
		this.gcsSinkBucket = gcsSinkBucket;
	}
   

}
