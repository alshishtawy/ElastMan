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
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A server that listens for connections form ElastMan sensors.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class ElastManServer extends Thread {
	
	private boolean controlMode;
	
	int server_port=4444;

	// the min, max, delta of YCSB VMs to create
	int ident_client_max=18;
	int ident_client_min=1;
	int ident_client_delta=1;
	
	// if true. YCSB will be created externally (manually)
	// the ident server will only collect data
	boolean ident_client_manual=false;
	// time in seconds between add/remove YCSB VMs
	int ident_client_delay=900;
	// periods to repeat. 1 --> max --> 1 is one period
	int ident_period=4;
	// sampling time
	int ident_sampling = 300;
	// run controller every x seconds
	int control_interval = 300;
	private Cluster cluster;
	
	public ElastManServer(boolean controlMode, Cluster cluster) { //if controlMode=false will do identification
		this.controlMode = controlMode;
		this.cluster = cluster;
	}
	
	@Override
	public void run() {

		
		// FIXME: change to setters and getters
		server_port = Props.server_port;
		ident_client_min = Props.ident_client_min;
		ident_client_max = Props.ident_client_max;
		ident_client_delta = Props.ident_client_delta;
		ident_client_delay = Props.ident_client_delay;
		ident_period = Props.ident_period;
		ident_sampling = Props.ident_sampling;
		ident_client_manual = Props.ident_client_manual;

		control_interval = Props.control_interval;
		
			
		
		
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(server_port);
		} catch (IOException e) {
			System.err.println("Could not listen on port: " + server_port);
			System.exit(1);
		}
		
		
		
		Sensor handler;
		if(!controlMode) { // start workload gen as well
			handler = new Sensor(ident_sampling, controlMode, cluster); // get data every x min
			if(!ident_client_manual) {
				IdentWorkLoad work = new IdentWorkLoad(handler,ident_client_min, ident_client_max, ident_client_delta, ident_period, ident_client_delay, cluster); // add/rem every y > x min
				work.start();
			}
		} else {
			handler = new Sensor(control_interval, controlMode, cluster); // get data every x min
			new ControlWorkLoad(handler, cluster).start();
		}
		
		boolean first = true;
		while(handler.identifying) {  // FIXME: this will not stop the ".accept()"

			try {
				Socket clientSocket = serverSocket.accept();
				handler.addClient(clientSocket);
				if(first) {
					first=false;
					handler.start();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			serverSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
