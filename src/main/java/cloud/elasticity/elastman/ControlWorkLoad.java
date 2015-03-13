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

import java.util.ArrayList;

/**
 * Workload Generator used to test the controller.
 * Day/Night pattern followed by a number of spikes.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class ControlWorkLoad extends Thread {
	
	
	
	
	private int ycsbCount=0;
	private ArrayList<work> workload;
	Sensor handler;
	Cluster cluster;
	
	public ControlWorkLoad(Sensor handler, Cluster cluster) {
		this.handler = handler;
		this.cluster = cluster;
		workload = new ArrayList<ControlWorkLoad.work>();
		workload.add(new work(0, 12));
		/// controller starts here
		workload.add(new work(40, 14));
		workload.add(new work(30, 15));
		workload.add(new work(30, 16));
		workload.add(new work(30, 17));
		workload.add(new work(30, 18));
		workload.add(new work(30, 19));
		workload.add(new work(30, 20));
		workload.add(new work(30, 21));
		workload.add(new work(30, 22));
		workload.add(new work(30, 23));
		workload.add(new work(30, 24));
		workload.add(new work(30, 25));
		workload.add(new work(30, 26));
		workload.add(new work(30, 25));
		workload.add(new work(30, 24));
		workload.add(new work(30, 23));
		workload.add(new work(30, 22));
		workload.add(new work(30, 21));
		workload.add(new work(30, 20));
		workload.add(new work(30, 19));
		workload.add(new work(30, 18));
		workload.add(new work(30, 17));
		workload.add(new work(30, 16));
		workload.add(new work(30, 15));
		workload.add(new work(30, 14));
		workload.add(new work(30, 13));
		workload.add(new work(30, 12));
		workload.add(new work(30, 11));
		workload.add(new work(30, 10));
		
		
//		workload.add(new work(0, 11));
		
		
		
		//Spikes
		workload.add(new work(60, 26));
		workload.add(new work(60, 10));
		
		workload.add(new work(60, 24));
		workload.add(new work(60, 12));
		
		workload.add(new work(60, 22));
		workload.add(new work(60, 14));
		
		workload.add(new work(60, 20));
		workload.add(new work(60, 16));
		
		workload.add(new work(60, 26));
		workload.add(new work(60, 10));
		
		
	}
	
	@Override
	public void run() {
		
		for (int i = 0; i <workload.size(); i++) {
			work w = workload.get(i);
			int n = w.getVms() - ycsbCount; // vms to add or remove
			sleep(w.getDelayMs());
			if(n>0) {
				createYCSB(n);
			} else if (n<0) {
				destroyYCSB(-1*n);
			}
			
		}
		
		System.out.println("Done Control Test");
	}
	
	private void sleep(int delayMs) {
		try {
			Thread.sleep(delayMs);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	private void createYCSB(int n) {
		cluster.createVMs(n, cluster.getYcsbPrefix(), cluster.getYcsbImage(), cluster.getYcsbFlavor());
		ycsbCount += n;
		System.out.println("Add: " + ycsbCount);
	}
	
	private void destroyYCSB(int n) {
		for (int i = 0; i < n; i++) {
			if(ycsbCount>0) {
				ycsbCount--;
				handler.remLastClient();
				cluster.deleteVMs(cluster.getYcsbPrefix()+ycsbCount);
				System.out.println("Rem: " + ycsbCount);
			} else {
				System.out.println("Err: " + ycsbCount);
			}
		}
		
	}
	
	class work {
		int delayMs;	// how much to wait in ms
		int vms;	// how many vms to add/remove +/-'
		public work(int delayMinuts, int vms) {
			this.delayMs = delayMinuts * 60 * 1000;
			this.vms = vms;
		}
		public int getDelayMs() {
			return delayMs;
		}
		
		public int getVms() {
			return vms;
		}
	}

}
