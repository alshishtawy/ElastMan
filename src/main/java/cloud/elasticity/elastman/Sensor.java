/*
 * This file is part of the ElastMan Elasticity Manager
 * 
 * Copyright (C) 2013 Ahmad Al-Shishtawy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package cloud.elasticity.elastman;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class Sensor extends Thread {
	
	static Logger log = LoggerFactory.getLogger(Sensor.class);
	
	boolean controlMode;

	ArrayList<MyIO> clientSockets;
	ArrayList<MyIO> deadSockets;
	
	SummaryStatistics read_op, read_mean, read_stddiv, read_min, read_p95, read_p99, read_max;
	SummaryStatistics mixed_op, mixed_mean, mixed_stddiv, mixed_min, mixed_p95, mixed_p99, mixed_max;
	SummaryStatistics total_op;
	public boolean identifying = true;
	private int warmup = 2;	// iterations to wait before controller
//	private long rebalancing = 0;
	private Actuator actuator=null;
	private double ffThroughputDelta; // larger delta will indicate a spike thus we use FF
	
	final long period; // in seconds
	long timeStep=0;
//	SimpleBinaryClassifier ff = new SimpleBinaryClassifier(1800, 200, 0, 1000); // FF model
	SimpleBinaryClassifier ff; // FF model
	
	private long nextFF=0;
	
	private double inOp;
	private double outOp;
	
	private PIDController pid;
	private Filter filter;
	
	
	private Cluster cluster;
	
	public Sensor(int sleepSec, boolean controlMode, Cluster cluster) { // if controlMode=false will do identification
		clientSockets = new ArrayList<MyIO>();
		deadSockets = new ArrayList<MyIO>();
		
		read_op = new SummaryStatistics();
		read_mean = new SummaryStatistics();
		read_stddiv = new SummaryStatistics();
		read_min = new SummaryStatistics();
		read_p95 = new SummaryStatistics();
		read_p99 = new SummaryStatistics();
		read_max = new SummaryStatistics();
		
		mixed_op = new SummaryStatistics();
		mixed_mean = new SummaryStatistics();
		mixed_stddiv = new SummaryStatistics();
		mixed_min = new SummaryStatistics();
		mixed_p95 = new SummaryStatistics();
		mixed_p99 = new SummaryStatistics();
		mixed_max = new SummaryStatistics();
			
		total_op = new SummaryStatistics();
		
		period = sleepSec;
		this.controlMode = controlMode;
		this.cluster=cluster;
		
		
		
		
		// TODO: change normalized setPoint to real setPoint
		pid = new PIDController(Props.control_inOp, Props.control_outOp, 0, Props.control_kp, Props.control_ki, Props.control_kd);
		
		this.inOp = Props.control_inOp;
		this.outOp = Props.control_outOp;
		
		actuator = new Actuator(cluster, Props.voldMin, Props.voldMax, Props.voldDeltaMax, Props.createVMs);
		
		
		filter = new Filter(Props.filter_alpha);
		warmup = Props.control_warmup;
		dead = Props.control_dead;
		ffThroughputDelta = Props.control_ff_throughputDelta;
		
		
		ff = new SimpleBinaryClassifier(Props.control_ffr1, Props.control_ffw1, Props.control_ffr2, Props.control_ffw2);
		
	}

	public synchronized void addClient(Socket cs) {
		try {
			cs.setSoTimeout(40000); // wait max 40 seconds;
		} catch (SocketException e) {
			log.error("Error: Can't set socket time out!");
			log.error(e.getMessage());
		} 
		clientSockets.add(new MyIO(cs));
		log.info("Client added: {}", cs.getInetAddress());
	}
	
	public synchronized void remLastClient() {
		if(clientSockets.size()==0){
			log.error("No More Clients!!");
			return;
		}
		
		MyIO cs = clientSockets.get(clientSockets.size()-1);
		log.info("Removing Client: " + cs.s.getInetAddress());
		try {
			cs.s.close();
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		clientSockets.remove(clientSockets.size()-1);
		
	}
	
	
	@Override
	public void run() {
		
		String filename;
		if(controlMode) {
			filename = "control.dat";
		} else {
			filename = "ident.dat";
		}
		
		// Open the data file
		FileWriter fstream;
		BufferedWriter out=null;
		try {
			fstream = new FileWriter(filename);
			out = new BufferedWriter(fstream);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  
		// Write a header  
		try {
			out.write("T \t" +
					"PeriodSec \t" +
					"Clients \t" +
					"Servers \t" +
					"TotalOps \t" +
					"Throughput \t" +
					"ThroPerServ \t" +
					
					"tpsR \t" +
					"meanR \t" +
					"stddivR \t" +
					"minR \t" +
					"p95R \t" +
					"p99R \t" +
					"fp99R \t" +
					"maxR \t" +
					
					"tpSM \t" +
					"meanM \t" +
					"stddivM \t" +
					"minM \t" +
					"p95M \t" +
					"p99M \t" +
					"maxM \t" +
					"ntp \t" + 
					"nfp99 \t" +
					"opID \t" +
					"log\n");
			
			out.flush();
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		
		boolean firstInput = true;
		double lastTps = 0;
		boolean bigTPChange=false;
		
		while(identifying) {
			if(warmup > 0){
				warmup--;
			} else if (warmup==0) {
				warmup--;
//				lastTimeSec = System.nanoTime() / 1000000000; // for the controller  // NOT USED 
			}
				
				
			long start = System.nanoTime();
			
			//// sleep for sampling time then collect data
			try {
				Thread.sleep(period * 1000);
			} catch (InterruptedException e) {
				log.error(e.getMessage());
			}
			timeStep++;
			// loop and fetch data from each YCSB client
			updateMonitoringData();
			long end = System.nanoTime();
			long pInSec = (end-start)/1000000000;	// sampling period in seconds
			
			
			
			
			final double throughput = total_op.getSum()/pInSec;
			
			// Throughput per server
			final double tps = throughput/cluster.getActiveVoldVMsCount();
			// Read Throughput per server
			final double rtps = read_op.getSum()/pInSec/cluster.getActiveVoldVMsCount();
			// Write Throughput per server
			final double mtps = mixed_op.getSum()/pInSec/cluster.getActiveVoldVMsCount();
			
			// calculate a smoothed value of the p99 as well
			filter.step(read_p99.getMean());
			
			if(firstInput) {
				lastTps = tps;
				firstInput=false;
			}
			

			log.debug("Summary: " +timeStep + " \t" + pInSec + " \t" + (clientSockets.size() - deadSockets.size()) + " \t" + cluster.getActiveVoldVMsCount() + " \t" + total_op.getSum() + " \t" + (long)(throughput) + " \t" + (long)(throughput/cluster.getActiveVoldVMsCount())
					+ " \t" + rtps+ " \t" + (long)read_mean.getMean() + " \t" + (long)read_stddiv.getMean() + " \t" + (long)read_min.getMean() 
					+ " \t" + (long)read_p95.getMean()+ " \t" +(long)read_p99.getMean()+ " \t" + (long)filter.getValue() + " \t" + (long)read_max.getMean()
					+ " \t" + mtps+ " \t" + (long)mixed_mean.getMean() + " \t" + (long)mixed_stddiv.getMean() + " \t" + (long)mixed_min.getMean()
					+ " \t" + (long)mixed_p95.getMean()+ " \t" +(long)mixed_p99.getMean()+ " \t" + (long)mixed_max.getMean()
					+ " \t" +  (long)((throughput/cluster.getActiveVoldVMsCount())-outOp) + " \t" + (long)(filter.getValue() - inOp) );
			
			try {
				out.write("" +timeStep + " \t" + pInSec + " \t" + (clientSockets.size() - deadSockets.size()) + " \t" + cluster.getActiveVoldVMsCount() + " \t" + total_op.getSum() + " \t" + (long)(throughput) + " \t" + (long)(throughput/cluster.getActiveVoldVMsCount())
						+ " \t" + (long)rtps+ " \t" + (long)read_mean.getMean() + " \t" + (long)read_stddiv.getMean() + " \t" + (long)read_min.getMean() 
						+ " \t" + (long)read_p95.getMean()+ " \t" +(long)read_p99.getMean()+ " \t" + (long)filter.getValue() + " \t" + (long)read_max.getMean()
						+ " \t" + (long)mtps+ " \t" + (long)mixed_mean.getMean() + " \t" + (long)mixed_stddiv.getMean() + " \t" + (long)mixed_min.getMean()
						+ " \t" + (long)mixed_p95.getMean()+ " \t" +(long)mixed_p99.getMean()+ " \t" + (long)mixed_max.getMean()
						+ " \t" +  (long)((throughput/cluster.getActiveVoldVMsCount())-outOp) + " \t" + (long)(filter.getValue() - inOp)
						+ " \t"); 
				if(!controlMode) {
					out.write( "-1 \tIdent\n");
					out.flush();
				} // else -> later append control log and flush
						
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("======================");
			
			//clear stats
			read_op.clear();
			read_mean.clear();
			read_stddiv.clear();
			read_min.clear();
			read_p95.clear();
			read_p99.clear();
			read_max.clear();
			
			mixed_op.clear();
			mixed_mean.clear();
			mixed_stddiv.clear();
			mixed_min.clear();
			mixed_p95.clear();
			mixed_p99.clear();
			mixed_max.clear();
				
			total_op.clear();
			
			// remove dead clients
			if(deadSockets.size()>0) {
				clientSockets.removeAll(deadSockets);
				deadSockets.clear();
				System.out.println("Removind Dead Sockets!");
			}
			if(!controlMode && clientSockets.size()==0) {
				identifying = false; // finished the identification
				System.out.println("Identification completed" );
			}
			if(warmup == 0) {  // next time the controller will be started!! so initialize;
				pid.reset();
				filter.reset();	// to remove any noise in startup
			}
			
			
			
			
			if(controlMode && warmup >= 0) {
				try {
					out.write("0 \tWarmup\n");
					out.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if(controlMode && warmup < 0) {
				
				
				if( !isRebalancing() && Math.abs(lastTps-tps)>ffThroughputDelta) {
					bigTPChange=true;
					System.out.println("Big Throughput Change: " + (lastTps-tps));
				}
				
				
				// 0 - check
				cluster.updateVMs();
				if(actuator.isCreateVMs() && cluster.getActiveVoldVMsCount() != cluster.getVoldVMsCount()) { // then there is something wrong (e.g., didn't finish removing nodes)
					System.out.println("Vold Count Error!!");	// Should never happen unless someone adds VoldVMs externally
					pid.reset();
					filter.reset();
					try {
						out.write("3 \tRebalanceNotComplete!\n");
						out.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				// TODO: 1 - Error is very large for first time then do nothing
//				else if (firstLargeValue && !(Math.abs(lastTps-tps)>ffThroughputDelta))  {	// this is probably noise, Ignore it
//					// do nothing
////					pidReset(filter);
//					System.out.println("Controller: Very large value for first time! Do nothing!");
//					try {
//						out.write("4 \tFirstLarge\n");
//						out.flush();
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
				// 2 - if in dead zone then do nothing
				else if(inOp-2*dead <= filter.getValue()  && filter.getValue() <= inOp+dead){
					System.out.println("Controller: in dead zone! Do nothing!");
					pid.reset();
					filter.reset();
					try {
						out.write("0 \tDeadZone\n");
						out.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				// 3 - Rebalancing
				else if(isRebalancing()) {
					System.out.println("Controller: Rebalancing! Do nothing!");
//					pidReset(filter);
					try {
						out.write("3 \tRebalancing\n");
						out.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					  // FIXME: now I give rebalance 2 period to finish.
										// Should check the real status and update rebalance accordingly
				}
				// 3.5 - if current latency is less than desired and min servers if 3 then do nothing.
				else if (cluster.getActiveVoldVMsCount()<=3 && filter.getValue() <= inOp+dead){ // should never be < 3
					System.out.println("Controller: Having min=3 Vold VMs and the response time is OK! Not running controller");
					pid.reset();
					filter.reset();
					try {
						out.write("0 \tMinVMs\n");
						out.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} // 4 - 
				else {
					boolean usePID=true, ffFail=false;
					
					if (timeStep>nextFF && (  bigTPChange ||  (filter.getValue()>(inOp + (inOp*0.5)) || filter.getValue()<(inOp - (inOp*0.5)))    ) /*Big change in load use ff*/){//(filter>(inOp + (inOp*0.5)) || filter<(inOp - (inOp*0.5)))) {
						usePID=false;
						bigTPChange=false;
						//	use binary classifier
						nextFF = timeStep+4;	// TODO: Fix nextFF
						System.out.println("Controller: Using FF");
						double output= ff.classify(rtps, mtps);
						// calculate number of servers needed to handle current throughput
						double n = (throughput/output) - cluster.getActiveVoldVMsCount();

						// TODO: Now I get ceil. Check if there is a better solution
						int nn =0;
//						if(n>0) {
							nn=(int)Math.ceil(n);
//						} else {
//							nn=(int)Math.floor(n);
//						}
						
						//int nn = (int)Math.round(n);
						
						System.out.println("Controller: FF output = " + output + " that is "+ n + " -> " + nn + " servers");

						if((filter.getValue()>(inOp+inOp*0.5) && nn<3) || (filter.getValue()<(inOp-inOp*0.5) && nn>-3) ) {//Math.abs(nn)<3) {
							// Very large error & add/rem few VMs! Must be outside of op region
							// Fall back to FB
							usePID=true;
							ffFail=true;
						} else {

							try {
								out.write("2 \tFF#"+output+"#"+n+"#"+nn+"\n");
								out.flush();
								pid.reset();
								filter.reset();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if(nn > 0 || (nn < 0 && cluster.getActiveVoldVMsCount() > 3)) {
								actuator.scheduleRebalance(nn,false);
								
							}
						}
					}
					if(usePID){ // 4 - use PID
						System.out.println("Controller: Using FB");
						double delta = pid.step(filter.getValue());  // pid gives throughput per server
						
						double output = tps+delta;	// this is the new throughput per server
						if(output < 50) {
							output=50;
							System.err.println("WARNING!!! pid gave negative/small output!!");
						}

						// calculate number of servers needed to handle new throughput
						double n = (throughput/output) - cluster.getActiveVoldVMsCount();

						// TODO: Now I ceil. Check if there is a better solution
						int nn =0;
//						if(n>0) {
							nn=(int)Math.ceil(n);
//						} else {
//							nn=(int)Math.floor(n);
//						}// int nn = (int)Math.round(n);

						System.out.println("Controller: PID output = " + output + " that is "+ n + " -> " + nn + " servers");

						try {
							out.write("1 \tFB#"+output+"#"+n+"#"+nn);
							if(ffFail) {
								out.write("#FFFail");
							}
							out.write("\n");
							out.flush();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if(nn > 0 || (nn < 0 && cluster.getActiveVoldVMsCount() > 3)) {
							actuator.scheduleRebalance(nn,true);
						}
					}
				}
			}
			
			lastTps = tps;
		}
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private synchronized void updateMonitoringData() { // sync not to allow adding clients while in the loop
		for (MyIO io : clientSockets) {
			// Read operations
			long NR=0;
			double meanR=0, stddivR=0, minR=0, p95R=0, p99R=0, maxR=0;
			// Mixed operations (read/write transactions) 
			long NM=0;
			double meanM=0, stddivM=0, minM=0, p95M=0, p99M=0, maxM=0;

			try {
				io.out.writeInt(0);	//ping the client to send data. client is waiting for any int from us
				NR = io.in.readLong();
				meanR = io.in.readDouble();
				stddivR = io.in.readDouble();
				minR = io.in.readDouble();
				p95R = io.in.readDouble();
				p99R = io.in.readDouble();
				maxR = io.in.readDouble();

				NM = io.in.readLong();
				meanM = io.in.readDouble();
				stddivM = io.in.readDouble();
				minM = io.in.readDouble();
				p95M = io.in.readDouble();
				p99M = io.in.readDouble();
				maxM = io.in.readDouble();
				
				if(NR==0) {  // This is a new client that did not start yet! Don't add this time
					return;
				}

				// if everything went fine with no exceptions then add to stats, otherwise the socket will be removed after the loop
				read_op.addValue(NR);
				read_mean.addValue(meanR);
				read_stddiv.addValue(stddivR);
				read_min.addValue(minR);
				read_p95.addValue(p95R);
				read_p99.addValue(p99R);
				read_max.addValue(maxR);

				mixed_op.addValue(NM);
				mixed_mean.addValue(meanM);
				mixed_stddiv.addValue(stddivM);
				mixed_min.addValue(minM);
				mixed_p95.addValue(p95M);
				mixed_p99.addValue(p99M);
				mixed_max.addValue(maxM);

				total_op.addValue(NR+NM);

				log.debug((NR+NM)+" \t"+NR+" \t"+meanR+" \t"+stddivR+" \t"+minR+" \t"+p95R+" \t"+p99R+" \t"+maxR+" \t"+NM+" \t"+meanM+" \t"+stddivM+" \t"+minM+" \t"+p95M+" \t"+p99M+" \t"+maxM);
			} catch (IOException e) {
				
				// TODO: I'm not removing dead clients since the workload generator removes them in remLastClient()
				//System.out.println("Error: Dead Client NOT added to deadlist for removal.");	// Should not happen in normal system
				//deadSockets.add(io);	// to remove dead clients later. The workload generator just stops the VM running the client
				
				log.error(e.getMessage());
			}

		}

	}

//	private long lastTimeSec = 0;
//	private double lastInput = 0;
//	private double lastError=0; 
//	private double outMin=-4000, outMax=4000; 	// TODO: check values of outMax and inMax

//	private double mySetpoint = 0; // in nano // TODO: set the setpoint?
//	private double kp, ki, kd, iTerm=0; // TODO: set the real gains

	
	private double dead; 
	

	


	
	class MyIO {
		public DataInputStream in=null;
		public DataOutputStream out=null;
		public Socket s=null;
		public MyIO(Socket s) {
			this.s = s;
			try {
				in = new DataInputStream(s.getInputStream());
				out = new DataOutputStream(s.getOutputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private boolean isRebalancing() {
		if(actuator==null) {
			return false;
		}
		
		return actuator.isRebalancing();	
	}

	
//	// delete me: Testing pid! A quick hack to test the pid controller
//	public static void main(String[] args) {
//		
//		FileInputStream propFile;
//		try {
//			propFile = new FileInputStream( "control.prop");
//			ElastManServer.controlProps = new Properties(System.getProperties());
//			ElastManServer.controlProps.load(propFile);
//		} catch (FileNotFoundException e1) {
//			System.err.println("Control properties not found!! using defaults");
//			e1.printStackTrace();
//		} catch (IOException e) {
//			System.err.println("Control properties IO error!! using defaults");
//			e.printStackTrace();
//		}
//		
//		
//		Scanner scanner = new Scanner(System.in);
//		Sensor c = new Sensor(0, true);
//		
//		System.out.println("Testing PID: " +  c.kp + ", " + c.ki + ", " + c.kd);
//		System.out.print("Enter First Input: ");
//		
//		Double in = scanner.nextDouble();
////		c.lastInput = in - c.inOp;
//		c.pidReset(in);
//		
//		while(true) {
//			Double out = c.pid(in);
//			System.out.println("Controller says: " + out);
//			System.out.print("Enter Next Input: ");
//			in = scanner.nextDouble();
//		}
//	}
	
}
