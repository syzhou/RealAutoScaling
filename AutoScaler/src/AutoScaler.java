import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: pullrequest.proto

public abstract class AutoScaler extends AutoScalerRequest{
	String lbAddress;
	int lbPort;
	int pullInterval;
	Properties properties;
	String credentialPath;
	int initNumWorkers;
	int minNumWorkers;
	int maxNumWorkers;
	String ami;
	String securityGroup;
	String ec2KeyName;
	String instanceType;
	String configPath;
	String ec2KeyPath;
	String workerScriptPath;
	
	
	PullRequest.Builder requestBuilder;
	PullResponse response;

	Socket client = null;
	Map<Integer, Instance> activeWorkers = new HashMap<Integer, Instance>();
	Map<Integer, Instance> disabledWorkers = new HashMap<Integer, Instance>();

	BasicAWSCredentials bawsc;
	AmazonEC2Client ec2;
	int cooldown = 0;
	int tagCounter = 0;


	final Object requestBuilderLock = new Object();
	
	static final String lbAddressKeyName = "lbAddress";
	static final String lbPortKeyName = "lbPort";
	static final String pullIntervalKeyName = "pullIntervalMilli";
	static final String credentialKeyName = "credentialPath";
	static final String initNumWorkersKeyName = "startWith";
	static final String minNumWorkersKeyName = "minWorkers";
	static final String maxNumWorkersKeyName = "maxWorkers";
	static final String amiKeyName = "ami";
	static final String securityGroupKeyName = "securityGroup";
	static final String ec2KeyNameKeyName = "ec2KeyName";
	static final String instanceTypeKeyName = "instanceType";
	static final String ec2KeyPathKeyName = "ec2KeyPath";
	static final String workerScriptPathKeyName = "workerScriptPath";
	
	static final int ec2CheckInterval = 10000;
	static final int ec2WaitTime = 20000;
	static final int cooldownInterval = 10000;
	
	
	
	private void initFromProperty() {
		System.out.println("initilzing properties from config file");
		// Read the configuration file
		properties = new Properties();
		try {
			FileInputStream propertyInStream = new FileInputStream(new File(configPath));
			properties.load(propertyInStream);
			propertyInStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Initialize variables
		lbAddress = properties.getProperty(lbAddressKeyName);
		System.out.println("lbAddress\t" + lbAddress);
		lbPort = Integer.parseInt(properties.getProperty(lbPortKeyName));
		System.out.println("lbPort\t" + lbPort);
		pullInterval = Integer.parseInt(properties.getProperty(pullIntervalKeyName, "1000"));
		System.out.println("pullInterval\t" + pullInterval);
		credentialPath = properties.getProperty(credentialKeyName);
		System.out.println("credentialPath\t" + credentialPath);
		initNumWorkers = Integer.parseInt(properties.getProperty(initNumWorkersKeyName, "2"));
		System.out.println("initNumWorkers\t" + initNumWorkers);
		minNumWorkers = Integer.parseInt(properties.getProperty(minNumWorkersKeyName, "2"));
		System.out.println("minNumWorkers:\t" + minNumWorkers);
		maxNumWorkers = Integer.parseInt(properties.getProperty(maxNumWorkersKeyName, "2"));
		System.out.println("maxNumWorkers\t" + maxNumWorkers);
		ami = properties.getProperty(amiKeyName);
		System.out.println("ami\t" + ami);
		securityGroup = properties.getProperty(securityGroupKeyName);
		System.out.println("securityGroup\t" + securityGroup);
		ec2KeyName = properties.getProperty(ec2KeyNameKeyName);
		System.out.println("ec2KeyName\t" + ec2KeyName);
		instanceType = properties.getProperty(instanceTypeKeyName);
		System.out.println("instanceType\t" + instanceType);
		ec2KeyPath = properties.getProperty(ec2KeyPathKeyName);
		System.out.println("ec2KeyPath\t" + ec2KeyPath);
		workerScriptPath = properties.getProperty(workerScriptPathKeyName);
		System.out.println("workerScriptPath\t" + workerScriptPath);

		
	}
	
	private void initAWS() {
		System.out.println("initializing AWS credentials");
		Properties credentialProperties = new Properties();
		try {
			FileInputStream credentialInStream = new FileInputStream(new File(credentialPath));
			credentialProperties.load(credentialInStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		bawsc = new BasicAWSCredentials(credentialProperties.getProperty("AWSAccessKeyId"), 
				credentialProperties.getProperty("AWSSecretKey"));
		//Create an Amazon EC2 Client
		ec2 = new AmazonEC2Client(bawsc);
	}
	
	
	public class WorkerInfo {
		public int tag;
		public int queueSize;
		
		public WorkerInfo(PullResponse.WorkerStat workerStat) {
			this.tag = workerStat.getTag();
			this.queueSize = workerStat.getQueueSize();
		}
	}
	
	public class WorkersInfo {
		public List<WorkerInfo> workers;
		
		@SuppressWarnings("unused")
		private WorkersInfo() {}
		public WorkersInfo(PullResponse response) {
			workers = new ArrayList<WorkerInfo>();
			List<PullResponse.WorkerStat> workerStats = response.getWorkersList();
			for (PullResponse.WorkerStat workerStat : workerStats) {
				workers.add(new WorkerInfo(workerStat));
			}
		}
		
		public float getAverageQueueSize() {
			float sum = 0;
			for (WorkerInfo worker : workers) {
				sum += worker.queueSize;
			}
			if (sum == 0) {
				return 0;
			}
			return (sum / workers.size());
		}
		
		public int getMaxQueueSize() {
			int max = 0;
			for (WorkerInfo worker : workers) {
				if (worker.queueSize > max) {
					max = worker.queueSize;
				}
			}
			return max;
		}
		
		public int getMinQueueSize() {
			int min = Integer.MAX_VALUE;
			for (WorkerInfo worker : workers) {
				if (worker.queueSize < min) {
					min = worker.queueSize;
				}
			}
			return min;
		}
	}
	
	abstract public void scale(WorkersInfo info);
	
	public void addWorker(int numNewWorkers, int cooldown) {
		System.out.println("add worker: numNewWorkers = " + numNewWorkers);
		int currNumWorkers = 0;
		synchronized (activeWorkers) {
			currNumWorkers = activeWorkers.size();
		}
		System.out.println("CurrentNumWorkers " + currNumWorkers);
		if (currNumWorkers >= maxNumWorkers) {
			System.out.println("Reaching maxNumWorkers, do nothing");
			return;
		}
		this.cooldown = cooldown;
		synchronized (activeWorkers) {
			for (Iterator<Map.Entry<Integer, Instance>> it = disabledWorkers.entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<Integer, Instance> entry = it.next();
				requestBuilder.addToRebornWorkers(entry.getKey());
				numNewWorkers--;
				activeWorkers.put(entry.getKey(), entry.getValue());
				it.remove();
				System.out.println("reborn  " + entry.getKey());
				if (numNewWorkers == 0) {
					break;
				}
			}
		}

		for (int i = 0; i < numNewWorkers && i < maxNumWorkers - currNumWorkers; i++) {
			new Thread(new WorkerLauncher()).start();
		}
	}
	public void removeWorker(int numRemoveWorkers, int cooldown) {
		System.out.println("remove worker: numRemoveWorkers = " + numRemoveWorkers);
		int currNumWorkers = 0;
		synchronized (activeWorkers) {
			currNumWorkers = activeWorkers.size();
		}
		System.out.println("CurrentNumWorkers " + currNumWorkers);
		if (currNumWorkers <= minNumWorkers) {
			System.out.println("reaching minNumWorkers, do nothing");
			return;
		}
		this.cooldown = cooldown;

		for (int i = 0; i < numRemoveWorkers && i < currNumWorkers - minNumWorkers; i++) {
			disableOneWorker();
		}
	}
	
	private void disableOneWorker() {
		int minWorker = 0;
		int minQueueSize = Integer.MAX_VALUE;
		List<PullResponse.WorkerStat> workers = response.getWorkersList();
		for (PullResponse.WorkerStat worker: workers) {
			if (worker.getQueueSize() < minQueueSize) {
				minQueueSize = worker.getQueueSize();
				minWorker = worker.getTag();
			}
		}
		synchronized (activeWorkers) {
			disabledWorkers.put(minWorker, activeWorkers.get(minWorker));
			if (activeWorkers.remove(minWorker) == null) {
				System.out.println("[ERROR]: worker " + minWorker + " isn't in active set");
			}
		}
		synchronized (requestBuilderLock) {
			requestBuilder.addToDisableWorkers(minWorker);
		}
	}
	
	class WorkerLauncher implements Runnable {

		@Override
		public void run() {
			System.out.println("lauching new worker");
			//Create Instance Request
			RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
			 
			//Configure Instance Request
			runInstancesRequest.withImageId(ami)
			.withInstanceType(instanceType)
			.withMinCount(1)
			.withMaxCount(1)
			.withKeyName(ec2KeyName)
			.withSecurityGroupIds(securityGroup);
			 
			//Launch Instance
			RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);  
			 
			//Return the Object Reference of the Instance just Launched
			Instance newInstance = runInstancesResult.getReservation().getInstances().get(0);
			while (true) {
				try {
					Thread.sleep(ec2CheckInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				newInstance = updateInstanceInfo(newInstance);
				if (newInstance.getState().getName().equals("running")
						&& newInstance.getPublicDnsName() != null) {
					System.out.println("new instance starts running:" 
								+ newInstance.getPublicDnsName());
					break;
				} else {
				}
			}
			while (true) {
				try {
					if (InetAddress.getByName(lbAddress).isReachable(1000)) {
						break;
					} else {
						System.out.println("new worker not reachable, sleep");
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			try {
				Thread.sleep(40000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int tag = 0;
			synchronized (activeWorkers) {
				tag = tagCounter;
				activeWorkers.put(tag, newInstance);
				tagCounter++;
			}
			activateNewWorker(newInstance, tag);
			
		}
	}
	
	private void activateNewWorker(Instance instance, int tag) {
		// TODO: ssh, pass tag and LB DNS
		System.out.println("activate new worker " + instance.getInstanceId());
		String instanceDNS = instance.getPublicDnsName();
		String cmd = "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i "
                + ec2KeyPath
                + " ubuntu@"
                + instanceDNS
                + " bash "
                + workerScriptPath  + " "
                + tag + " "
                + lbAddress + ":" + lbPort;
		System.out.println("command: " + cmd);
		try {
			Process p = Runtime
			        .getRuntime()
			        .exec(cmd);
			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	private Instance updateInstanceInfo(Instance instance) {
		DescribeInstancesRequest discribe =new DescribeInstancesRequest();
		List<String> describeInstanceList = new ArrayList<String>();
		describeInstanceList.add(instance.getInstanceId());
		discribe.setInstanceIds(describeInstanceList);
		List<Reservation> reservations = ec2.describeInstances(discribe).getReservations();
		instance = reservations.get(0).getInstances().get(0);
		
//		System.out.println(instance.getInstanceId());
//		System.out.println(instance.getState().getName());
//		System.out.println(instance.getPublicDnsName());
		return instance;
	}
	
	private void handleKillWorker(PullResponse response) {
		List<Integer> toKillWorkers = response.getToKillWorkersList();
		for (Integer toKillWorker : toKillWorkers) {
			Instance toKillInstance = disabledWorkers.get(toKillWorker);
			if (toKillInstance == null) {
				System.out.println("[ERROR] to kill worker: " + toKillWorker + " isn't in disabled set");
				continue;
			}
			terminateInstance(toKillInstance);
		}
	}
	
	// Terminate the instance
	private void terminateInstance(Instance instance) {
		TerminateInstancesRequest terminateLGrequest = new TerminateInstancesRequest();
		String instanceID = instance.getInstanceId();
		terminateLGrequest.withInstanceIds(instanceID);
		ec2.terminateInstances(terminateLGrequest);
		System.out.println(instanceID + " instance terminated");
	}
	
	private void CheckWorkerStatus() {

		try
		{
			long time = System.currentTimeMillis();
			System.out.println("Connecting to " + lbAddress + " on port " + lbPort);
			client = new Socket(lbAddress, lbPort);
			client.setTcpNoDelay(true);
			System.out.println("Just connected to " + client.getRemoteSocketAddress());
			sendInitRequest();
			requestBuilder = PullRequest.newBuilder().setUseless(0);
			while (true) {
				int currNumWorkers = 0;
				synchronized (activeWorkers) {
					currNumWorkers = activeWorkers.size();
				}
				System.out.println("[NUM] Minute:\t" + ((float)(System.currentTimeMillis() - time)) / 1000 / 60 + "\tnum:\t"+ currNumWorkers);
				synchronized (requestBuilderLock) {
					PullRequest request = requestBuilder.build();
					sendPullRequest(request);
					requestBuilder = PullRequest.newBuilder().setUseless(0);
				}
				response = receivePullResponse();
				List<PullResponse.WorkerStat> workers = response.getWorkersList();
				for (PullResponse.WorkerStat worker : workers) {
					System.out.println("worker " + worker.getTag() + "\tqueuesize " + worker.getQueueSize());
				}
				handleKillWorker(response);
				scale(new WorkersInfo(response));
				try {
					Thread.sleep(pullInterval);
					if (cooldown > 0) {
						while (cooldown > 0) {
							System.out.println("waiting for cooldown: " + cooldown / 1000 + "s");
							Thread.sleep(cooldownInterval);
							cooldown -= cooldownInterval;
							synchronized (activeWorkers) {
								currNumWorkers = activeWorkers.size();
							}
							
							System.out.println("[NUM] Minute:\t" + ((float)(System.currentTimeMillis() - time)) / 1000 / 60 + "num:\t"+ currNumWorkers);
						}
						cooldown = 0;
					}
					
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void setconfigPath(String configPath) {
		this.configPath = configPath;
	}
	
	private void launchInitWorkers() {
		addWorker(initNumWorkers, 0);
		while (true) {
			int currNumWorkers = 0;
			synchronized (activeWorkers) {
				currNumWorkers = activeWorkers.size();
			}
			System.out.println("waiting for first workers initilize, " 
					+ currNumWorkers + " is up");
			if (currNumWorkers == initNumWorkers) {
				break;
			}
			try {
				Thread.sleep(ec2CheckInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	public void run() {
		initFromProperty();
		initAWS();
		launchInitWorkers();
		CheckWorkerStatus();
	}
	private void sendInitRequest() {
		try {
			System.out.println("sending init request");
			OutputStream output = client.getOutputStream();
			byte[] buffer = new byte[8];
			buffer[0] = 9;
			for (int i = 1; i < 8; i++) {
				buffer[i] = 0;
			}
			output.write(buffer);
			System.out.println("init request sent");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendPullRequest(PullRequest request) {
		try {
			//System.out.println("Sending request to LB");
			OutputStream output = client.getOutputStream();
			ByteArrayOutputStream bo = new ByteArrayOutputStream(1024);
			request.writeTo(bo);
			byte[] buffer = bo.toByteArray();
			output.write(buffer);
			//System.out.println("request sent");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private PullResponse receivePullResponse() {
		PullResponse response = null;
		try {
			InputStream input = client.getInputStream();
			byte[] recvBuffer = new byte[1024];
			int retval = input.read(recvBuffer);
			byte[] dataBuffer = new byte[retval];
			for (int i = 0; i < retval; ++i)
				dataBuffer[i] = recvBuffer[i];
			response = PullResponse.parseFrom(dataBuffer);
			//System.out.println("response received");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return response;
	}
}