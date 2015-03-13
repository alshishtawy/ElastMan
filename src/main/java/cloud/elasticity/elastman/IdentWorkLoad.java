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

/**
 * Workload Generator used during system identification.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class IdentWorkLoad extends Thread {
	
	
	int min, max, delta;
	int periods;
	int sleepMS;
	Sensor handler;
	Cluster cluster;
	
	private int ycsbCount=0;
	public IdentWorkLoad(Sensor handler, int min, int max, int delta, int periods, int sleepSec, Cluster cluster) {
		this.max = max;
		this.min = min;
		this.delta=delta;
		this.periods = periods;
		this.sleepMS = sleepSec * 1000;
		this.handler = handler;
		this.cluster = cluster;
	}
	
	@Override
	public void run() {
		
		// create one
		int j = min; // now we created one
		createYCSB(j);
		sleep();
		
		for (int i = 0; i < periods; i++) {
			
			// increase from 1 to max
			for (; j < max; j+=delta) {
				createYCSB(delta);
				sleep();
			}
			
			// decrease from max to one
			
			for (; j>min; j-=delta) {
				destroyYCSB(delta);
				sleep();
			}
		}
		
		// delete last one
		destroyYCSB(j);
		j--;  // should be 0 now
	}
	
	private void sleep() {
		try {
			Thread.sleep(sleepMS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
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
	

}
