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

import java.io.IOException;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Actuator class is used to add or remove Voldemort nodes to the controlled Voldemort cluster.
 * The Actuator uses the rebalance tool provided by Voldemort to redistribute data.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class Actuator implements Runnable, Steppable {
	
	static Logger log = LoggerFactory.getLogger(Actuator.class);

//	private int nVMs;
//	private boolean limit;	// limit the max delta VMs to add or remove

	private LinkedList<Task> queue;
	
	Cluster cluster;
	
	private boolean createVMs;	// if false then actuator will only rebalance and leave VMs
	private int voldMax;	// Max vold cluster size
	private int voldMin;	// Min vold cluster size
	private int voldDeltaMax; // max number of VMs to add or remove at one time
	
	private boolean rebalancing = false;
	 
	
	public Actuator(Cluster cluster, int voldMin, int voldMax, int voldDeltaMax, boolean createVMs) {
		
		this.cluster = cluster;
		
		// FIXME: change to setters and getters instead of using Props to make it more generic
		this.createVMs = createVMs;
		this.voldMax = voldMax;
		this.voldMin = voldMin;
		this.voldDeltaMax = voldDeltaMax;
		
		queue = new LinkedList<Actuator.Task>();
	}
	
	public synchronized boolean  scheduleRebalance(int nVMs, boolean limit) {
		queue.add(new Task(nVMs, limit));
		if(rebalancing) {
			log.warn("The actuator is rebalancing! Can't have multiple rebalance instances at same time!");
			return false;
		} else {
			return true;
		}
	}
	
	private synchronized Task getNextTask() {
		if(queue.isEmpty()) {
			return null;
		}
		return queue.remove();
	}
	
	
	/**
	 * @param rebalancing
	 */
	private synchronized void setRebalancing(boolean rebalancing) {
		this.rebalancing = rebalancing;
	}
	
	/**
	 * Changes the rebalancing flag only if it is different than the current status. Otherwise, return false
	 * 
	 * @param rebalancing	The new state
	 * @return	true if state was changed. false if the new state is the same as the current state
	 */
	private synchronized boolean testAndSetRebalancing(boolean rebalancing){
		if(this.rebalancing == rebalancing) {
			return false;
		}
		this.rebalancing = rebalancing;
		return true;
	}
	
	/**
	 * @return
	 */
	public synchronized boolean isRebalancing() {
		return this.rebalancing;
	}
	
	public void run() {
		
		// Just an extra check; should not happen
		if(!testAndSetRebalancing(true)) {
			log.error("The actuator is rebalancing! Can not have two rebalance operations in parallel according to Voldemort specifications!");
			return;
		}
		Task t;
		while((t=getNextTask()) != null) {
			 
			int nVMs = t.getnVMs();
			boolean limit = t.getLimit();
			
			if(nVMs > 0) {  // add mode

				// 1 - Bound nVMs
				if(limit && nVMs > voldDeltaMax) {  // TODO: check if good idea
					nVMs = voldDeltaMax;  
				}

				if(cluster.getActiveVoldVMsCount()+nVMs > voldMax) {	// the max size allowed for the Voldemort store
					nVMs = voldMax - cluster.getActiveVoldVMsCount();
				}
				if(nVMs<=0) {  // will never be <0 but might equal 0
					setRebalancing(false);
					return;
				}
				cluster.setActiveVoldVMsCount(cluster.getActiveVoldVMsCount()+nVMs);
				// 2 - Create new VMs
				if(createVMs) {
					cluster.createVMs(nVMs, cluster.getVoldPrefix(), cluster.getVoldImage(), cluster.getVoldFlavor());
					// wait to finish creation
					cluster.waitCreating();

					// then wait a bit more for voldemort & os
					// FIXME: wait for things to finish
					try {
						Thread.sleep(2*60*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				//  3 - start rebalancing
				// cluster.getSync().reset();
				cluster.genCluster(cluster.getActiveVoldVMsCount());	// new cluster to move to
				//for (int i = 0; i < 5; i++) { // try rebalancing x times FIXME:This is not needed now after updating the rebalance script
				long rebStart = System.nanoTime();
				try {
					Process p=null;
					p=Runtime.getRuntime().exec("./myrebalance");
					p.waitFor();
				} catch (IOException e) {
					log.error("Rebalance didn't work!");
					log.error(e.getMessage());
				} catch (InterruptedException e) {
					log.error("Rebalance didn't work!");
					log.error(e.getMessage());
				} 
				long rebEnd = System.nanoTime();
				long rebTime = (rebEnd-rebStart)/1000000000; // in seconds
				log.info("Rebalance finished in {} sec", rebTime);
				//	if(rebTime > 30) {// if takes less that 30 secs then probably it failed!
				//		break;
				//	}
				//				try {
				//					Thread.sleep(1000);
				//				} catch (InterruptedException e) {
				//					e.printStackTrace();
				//				}
				//}
			} else if (nVMs < 0 && cluster.getActiveVoldVMsCount() > voldMin) { //remove only if I have more than 3 nodes

				// 1 - Bound nVMs
				if((cluster.getActiveVoldVMsCount() + nVMs)<voldMin) { // note that nVMs is negative, remove 
					nVMs = voldMin - cluster.getActiveVoldVMsCount();
				}

				if(limit && nVMs < -1*voldDeltaMax) {  // FIXME: check if good idea
					nVMs = -1*voldDeltaMax; // good to have lower bound  
				}
				if(nVMs>=0) {  // will never be >0 but might equal 0
					setRebalancing(false);
					return;
				}

				cluster.setActiveVoldVMsCount(cluster.getActiveVoldVMsCount()+nVMs);; // note that nVMs is negative

				// 2 - start rebalancing
				//			App.http("reset", "1");
				//			App.updateVMs();
				cluster.genCluster(cluster.getActiveVoldVMsCount());	// new cluster to move to
				//			for (int i = 0; i < 5; i++) { // try rebalancing x times FIXME:This is not needed now after updating the rebalance script
				long rebStart = System.nanoTime();
				try {
					Process p=null;
					p=Runtime.getRuntime().exec("./myrebalance");
					p.waitFor();
				} catch (IOException e) {
					log.error("Rebalance didn't work!");
					log.error(e.getMessage());
				} catch (InterruptedException e) {
					log.error("Rebalance didn't work!");
					log.error(e.getMessage());
				}
				long rebEnd = System.nanoTime();
				long rebTime = (rebEnd-rebStart)/1000000000; // in seconds
				System.out.println("Rebalance finished in " + rebTime + " sec");
				//				if(rebTime > 30) {// if takes less that 30 secs then probably it failed!
				//					break;
				//				}
				//				try {
				//					Thread.sleep(1000);
				//				} catch (InterruptedException e) {
				//					e.printStackTrace();
				//				}
				//			}



				// 3 - Delete extra VMs
				if(createVMs) {
					for (int i = cluster.getActiveVoldVMsCount()-nVMs-1; i >= cluster.getActiveVoldVMsCount(); i--) {  // nVMs is negative
						cluster.deleteVMs(cluster.getVoldPrefix()+i); 
						try {
							Thread.sleep(1000);	//don't delete too fast! maybe things will crash
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}
				}
			}

		
			// FIXME: Wait for system to settle after rebalance
			try {
				Thread.sleep(120*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		setRebalancing(false);

	}
	
	/**
	 * Process the current time step
	 *  
	 * @see cloud.elasticity.elastman.Steppable#step()
	 */
		
	public synchronized void step() {
		if(rebalancing || queue.isEmpty()) { 	// if no rebalance tasks then return
													// if a rebalance is ongoing then no need to start a
													// new instance as the current instance
													// will take care of tasks in the queue
			return;
		}
		new Thread(this).start();
	}
		
	
	/**
	 * @return
	 */
	public boolean isCreateVMs() {
		return createVMs;
	}
	
	/**
	 * @param createVMs
	 */
	public void setCreateVMs(boolean createVMs) {
		this.createVMs = createVMs;
	}
	
	class Task {
		private int nVMs;
		private boolean limit;
		
		public Task(int nVMs, boolean limit) {
			this.nVMs = nVMs;
			this.limit = limit;
		}
		
		public boolean getLimit() {
			return limit;
		}
		
		public int getnVMs() {
			return nVMs;
		}
		
	}


}
