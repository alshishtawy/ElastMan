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
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Cluster class is used to manage and one Voldemort cluster with ElastMan.
 * It manages the Voldemort and YCSB VMs on the cluster. 
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class Cluster {

	static Logger log = LoggerFactory.getLogger(Nova.class);


	private Nova nova;
	private SyncServer sync;


	private String voldPrefix = "vold";
	private String voldImage;
	private String voldFlavor;
	private String ycsbPrefix = "ycsb";
	private String ycsbImage;
	private String ycsbFlavor;
	private int replicationFactor = 3;

	// This is just for testing. Not used now
	String userData = "#!/bin/sh\n" + 
			"cd /home/ubuntu\n" + 
			"echo \"Hello World.  Voldemort Test. The time is now $(date -R)\" | tee test.txt\n";
	//			"wget http://kalle.sics.se:7811/cloud/dummy.conf\n" + 
	//			"curl http://kalle.sics.se:7811/webcache/voldcache.php?addVoldNode=`hostname | tr -d [:alpha:]`_`hostname -I`\n";




	// myVMs is a hashtable with key = (physical host ID) and value = (array list of servers (VMs) running on that physical host)
	private Hashtable<String, ArrayList<Server>> myVMs = new Hashtable<String, ArrayList<Server>>();
	private ArrayList<String> myHosts = new ArrayList<String>();

	private int myVMsCount = 0;
	private int myVoldVMsCount = 0;
	private int myYcsbVMsCount = 0;
	
	/**
	 * The number of VoldVMs that are participating in the store (have partitions assigned to them).
	 * In a Voldemort cluster it is possible to have empty nodes that are part of the cluster but
	 * not serving any data.
	 * 
	 * Currently this is not calculated. The initial value should be given then the Actuator updates it as
	 * Vold nodes are activated/deactivated. This is mainly used when the Actuator createVMs is set to false.
	 */
	private int activeVoldVMsCount = 0;


	/**
	 * Constructor for Cluster object.
	 * 
	 * @param nova	The Nova wrapper object that will be used with this cluster
	 * @param sync	The SyncServer that will be used with this cluster
	 */
	public Cluster(Nova nova, SyncServer sync) {
		super();
		this.nova = nova;
		this.sync = sync;
		activeVoldVMsCount= Props.voldCount;
		
	}



	public void updateVMs() {
		myVMs.clear();
		int our=0, vold=0, ycsb=0, other=0;

		for(Server s : nova.getVMs()){
			// VMs that uses our ssh-key for authentication are counted and stored in myVMs
			// Other VMs by the user are not managed by this class
			if (nova.getKeyName().equals(s.getKeyName())) {

				// Update the counters
				our++;
				if(s.getName().startsWith(voldPrefix)) {
					vold++;
				} else if (s.getName().startsWith(ycsbPrefix)) {
					ycsb++;
				}

				// Then add the current server (i.e., VM) to myMVs
				// myVMs is a hashtable with key = physical host ID and value = array list of servers (VMs) running on that physical host
				String tmpHostId = s.getHostId();  // the physical machine (host) where the VM runs
				ArrayList<Server> tmpServers= myVMs.get(tmpHostId);
				if(tmpServers != null) {
					tmpServers.add(s);
				} else {
					tmpServers = new ArrayList<Server>();
					tmpServers.add(s);
					myVMs.put(tmpHostId, tmpServers);
				}
			} else {
				other++;	// ignore other VMs that don't belong to us and don't add them to myVMs
			}

		}


		// update the list of hosts running myVMs
		myHosts.clear();
		for (String name : myVMs.keySet()) {
			myHosts.add(name);
		}

		myVMsCount = our;
		myVoldVMsCount = vold;
		myYcsbVMsCount = ycsb;
		log.info("Done updating VMs list. Our VMs = {}, vold = {}, ycsb = {}, Other VMs = {}, hosts = {}", new Object[]{myVMsCount, myVoldVMsCount, myYcsbVMsCount, other, myHosts.size()});

	}	



	/**
	 * Get the number of active Voldemort VMs in the cluster.
	 * 
	 * Usually the value is initialized to the initial number of active VMs then updated by an Actuator.
	 * This method does not actually query the Voldemort cluster. It relies on the Actuator to keep it up to date.
	 * 
	 * @return The number of active Voldemort VMs in the cluster
	 */
	public int getActiveVoldVMsCount() {
		return activeVoldVMsCount;
	}
	
	/**
	 * Set the number of active Voldemort nodes in the cluster.
	 * 
	 * This method does not change the actual Cluster. This is just a counter to keep track of things.
	 * 
	 * @param activeVoldVMsCount
	 */
	public void setActiveVoldVMsCount(int activeVoldVMsCount) {
		this.activeVoldVMsCount = activeVoldVMsCount;
	}


	/**
	 * True only during the creation of VMs
	 */
	private boolean creating = false;

	synchronized void createVMs(final int count, String prefix, String img, String flv) {
		
		creating = true;

		// Update to get correct myVMs counters (myVMsCount, myVoldVMsCount, myYcsbVMsCount) used to assign name to new VMs
		updateVMs();

		// we lock the web server so new VMs wait till we are ready with the config files (cluster.xml)
		// the lock is checked by a script running on the new VMs
		// when done creating all VMs we generate the cluster.xml file containing IP address of the new VMs
		// then we unlock to indicate that we are done and new VMs can now download the cluster.xml from the web server
		sync.lock();

		final boolean genCluster;
		
		int startID; //zero-based index
		if(voldPrefix.equals(prefix)) {
			genCluster=true;	// Only generate cluster file if Voldemort VMs are created
			startID = myVoldVMsCount;
		} else if (ycsbPrefix.equals(prefix)) {
			genCluster=false;
			startID = myYcsbVMsCount;
		} else {
			genCluster=false;
			startID = myVMsCount;
		}
		
		nova.createVMs(count, prefix, startID, img, flv, new Runnable() {
			
			public void run() {
				log.info("All new VMs are now created and ready!!!!!!!!!!!");
				updateVMs();
				// generating the cluster config file
				if(genCluster) {
					sync.reset();
					genCluster(-1);
				}
				// then unlock the web server so VMs can start downloading cluster.xml
				sync.unlock();
				creating = false;
				synchronized (nova) {
					nova.notifyAll();
				}
				
			}
		});
	}
	
	
	/**
	 * The method will block till VMs are created
	 */
	public void waitCreating() {
		while (creating) {
			synchronized (nova) {
				try {
					nova.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}


	void deleteVMs(String prefix) {

		updateVMs();
		int hosts=0, vms=0;
		boolean print = true; // to print host name only if found VMs to delete
		log.info("===========================");
		for (Entry<String, ArrayList<Server>> e : myVMs.entrySet()) {
			for(Server s : e.getValue()) {
				if(prefix == null || s.getName().startsWith(prefix)) {
					if(print) {
						log.info(getHostName(e.getKey()));
						hosts++;
						print= false;
					}
					log.info("\tDeleting VM: {}@{}", s.getName(),  Nova.getAddr(s));
					nova.deleteVM(s.getId());
					vms++;
				}
			}
			if(print==false) {
				log.info("===========================");
				print=true;
			}
		}
		log.info("Deleted {} VMs running on {} hosts!", vms, hosts);
	}
	
	

	void genCluster(int n) {
		updateVMs();
		if(n <= 0) {
			n = myVoldVMsCount;
		}

		StringBuilder cluster = new StringBuilder(); //maybe better
		//    	StringWriter cluster = new StringWriter();

		cluster.append("<?xml version=\"1.0\"?>");
		cluster.append("<cluster>\n");

		cluster.append("  <name>VoldCluster</name>\n");

		/// write
		sync.clusterCreate(cluster.toString());
		cluster = new StringBuilder();

		///////////////// zones

		//    	int zones = myVMs.size();

		//    	for (int i = 0; i < REPLICATION_FACTOR; i++) {
		//    		cluster.append("  <zone>\n");
		//    		cluster.append("    <zone-id>" + i + "</zone-id>\n");
		//    		cluster.append("    <proximity-list>" + (i%REPLICATION_FACTOR) + "</proximity-list>\n");
		//    		cluster.append("  </zone>\n");
		//    		/// write
		//        	sync.clusterAppend(cluster.toString());
		//        	cluster = new StringBuilder();
		//    	}

		cluster.append("  <zone>\n");
		cluster.append("    <zone-id>0</zone-id>\n");
		cluster.append("    <proximity-list>1,2</proximity-list>\n");
		cluster.append("  </zone>\n");
		/// write
		sync.clusterAppend(cluster.toString());
		cluster = new StringBuilder();
		cluster.append("  <zone>\n");
		cluster.append("    <zone-id>1</zone-id>\n");
		cluster.append("    <proximity-list>2,0</proximity-list>\n");
		cluster.append("  </zone>\n");
		/// write
		sync.clusterAppend(cluster.toString());
		cluster = new StringBuilder();
		cluster.append("  <zone>\n");
		cluster.append("    <zone-id>2</zone-id>\n");
		cluster.append("    <proximity-list>0,1</proximity-list>\n");
		cluster.append("  </zone>\n");
		/// write
		sync.clusterAppend(cluster.toString());
		cluster = new StringBuilder();

		ArrayList<ArrayList<Integer>> partitions = PartitionGenerator.calc(90, n);


		Collections.sort(myHosts);	// to get same zones everytime

		for (Entry<String, ArrayList<Server>> e : myVMs.entrySet()) {
			int z = myHosts.indexOf(e.getKey())%replicationFactor;	// get the zone id
			for (Server s : e.getValue()) {	// for each server in this zone
				String name = s.getName();
				if(!name.startsWith(voldPrefix)) {
					continue;
				}

				int id = Integer.parseInt(name.substring(voldPrefix.length()));		// ID of this vold server to get the partitions
				if(id >= partitions.size()) {
					continue; // for the case of delete VMs. I need cluster file with fewer vms
				}
				cluster.append("  <server>\n");
				cluster.append("    <id>" + id + "</id>\n");
				cluster.append("    <host>" + Nova.getAddr(s) + "</host>\n");
				cluster.append("    <http-port>8081</http-port>\n");
				cluster.append("    <socket-port>6666</socket-port>\n");
				cluster.append("    <admin-port>6667</admin-port>\n");
				//    			<!-- A list of data partitions assigned to this server -->
				cluster.append("     <partitions>");
				for (int i = 0; i <partitions.get(id).size(); i++) {
					cluster.append(partitions.get(id).get(i).toString());
					if(i != partitions.get(id).size()-1) {
						cluster.append(",");
					}
				}
				cluster.append("</partitions>\n");
				cluster.append("     <zone-id>" + z + "</zone-id>\n");
				cluster.append(" </server>\n");

				/// write
				sync.clusterAppend(cluster.toString());
				cluster = new StringBuilder();

			}
		}

		cluster.append("</cluster>\n");

		/// write
		sync.clusterAppend(cluster.toString());
		cluster = new StringBuilder();
	}


	/**
	 * Get the current Nova wrapper used.
	 * 
	 * @return The current Nova object used.
	 */
	public Nova getNova() {
		return nova;
	}


	/**
	 * Change the Nova wrapper
	 * @param nova The new Nova wrapper object.
	 */
	public void setNova(Nova nova) {
		this.nova = nova;
	}


	/**
	 * Get the current SyncServer used.
	 * 
	 * @return The current SyncServer.
	 */
	public SyncServer getSync() {
		return sync;
	}


	/**
	 * Change the SyncServer
	 * 
	 * @param sync The new SyncServer object.
	 */
	public void setSync(SyncServer sync) {
		this.sync = sync;
	}


	/**
	 * The prefix used to name new Voldemort VMs.
	 * 
	 * @return The current name prefix for Voldemort VMs.
	 */
	public String getVoldPrefix() {
		return voldPrefix;
	}


	/**
	 * The prefix used to name new Voldemort VMs
	 * 
	 * @param voldPrefix The new name prefix.
	 */
	public void setVoldPrefix(String voldPrefix) {
		this.voldPrefix = voldPrefix;
	}


	/**
	 * The prefix used to name new YCSB VMs
	 * 
	 * @return The current name prefix for YCSB VMs.
	 */
	public String getYcsbPrefix() {
		return ycsbPrefix;
	}

	/**
	 * The prefix used to name new YCSB VMs
	 * 
	 * @param ycsbPrefix The new name prefix.
	 */
	public void setYcsbPrefix(String ycsbPrefix) {
		this.ycsbPrefix = ycsbPrefix;
	}


	/**
	 * The replication factor used in the Voldemort store.
	 * @param replicationFactor The replication factor.
	 */
	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}



	

	/**
	 * @return
	 */
	public String getVoldImage() {
		return voldImage;
	}



	/**
	 * @param voldImage
	 */
	public void setVoldImage(String voldImage) {
		this.voldImage = voldImage;
	}



	/**
	 * @return
	 */
	public String getVoldFlavor() {
		return voldFlavor;
	}



	/**
	 * @param voldFlavor
	 */
	public void setVoldFlavor(String voldFlavor) {
		this.voldFlavor = voldFlavor;
	}



	/**
	 * @return
	 */
	public String getYcsbImage() {
		return ycsbImage;
	}



	/**
	 * @param ycsbImage
	 */
	public void setYcsbImage(String ycsbImage) {
		this.ycsbImage = ycsbImage;
	}



	/**
	 * @return
	 */
	public String getYcsbFlavor() {
		return ycsbFlavor;
	}



	/**
	 * @param ycsbFlavor
	 */
	public void setYcsbFlavor(String ycsbFlavor) {
		this.ycsbFlavor = ycsbFlavor;
	}



	/**
	 * Get the total number of VMs that belongs to the current user on the current
	 * cluster according to the last call to updateVMs.
	 * 
	 * @return Total number of VMs.
	 */
	public int getVMsCount() {
		return myVMsCount;
	}

	/**
	 * Get the number of Voldemort VMs that belongs to the current user that are on the
	 * current cluster according to the last call to updateVMs.
	 * 
	 * @return The number of Voldemort VMs.
	 */
	public int getVoldVMsCount() {
		return myVoldVMsCount;
	}


	/**
	 * Get the number of Voldemort VMs that belongs to the current user that are on the
	 * current cluster according to the last call to updateVMs.
	 * 
	 * @return The number of Voldemort VMs.
	 */
	public int getYcsbVMsCount() {
		return myYcsbVMsCount;
	}

	/**
	 * Get a list of currently used host IDs according to the last call to updateVMs
	 * 
	 * @return A list of host IDs
	 */
	public Iterable<String> getHosts() {
		return myHosts;
	}

	/**
	 * Get the Server objects corresponding to the VMs running on a
	 * particular host according to the last call to updateVMs.
	 * 
	 * @param 	hostId The host ID.
	 * @return	A list of Server objects.
	 */
	public Iterable<Server> getVMsOnHost(String hostId) {
		return myVMs.get(hostId);
	}




	// From http://demobox.github.com/jclouds-maven-site-1.5.0/1.5.0/jclouds-multi/apidocs/org/jclouds/openstack/nova/domain/Server.html#getHostId%28%29
	// The OpenStack Nova provisioning algorithm has an anti-affinity property that attempts
	// to spread out customer VMs across hosts. Under certain situations, VMs from the same 
	// customer may be placed on the same host. hostId represents the host your cloud server
	// runs on and can be used to determine this scenario if it's relevant to your application.

	// This method maps the host ID to a meaningful name
	// Couldn't find a way to get the host name from OpenStack API

	static String getHostName(String hostID) {
		// didn't find a way to get host name. So I manually map the hash to the name :(
		if ("e350129059f2606dbe702b48ca07e6702d066cd86aee64948a838fa3".equals(hostID))
			return "cloud8";
		else if ("de826ee6b294178b6f59e9ad85c3fa56dd743b431830eb690e794146".equals(hostID))
			return "cloud9";
		else if ("1a01f12d1469c01dc09d5f26cf78d527417ee53eeb6c1c15570f3ede".equals(hostID))
			return "cloud10";
		else if ("39a60c2dad556bb7411b27de4d82cf747b530f404557c2b1282be8f3".equals(hostID))
			return "cloud11";
		else if ("60aacf14c8a1f5fd93f7d12d83f20c65d62c82da69ec3e737c654cea".equals(hostID))
			return "cloud12";
		else if ("d7df866b7be5764a29f3d2deda21aab261c841ab7aa64857fcfeb66c".equals(hostID))
			return "cloud13";
		else if ("03c2c904bf67e153bd5fcedd2413a763b6cd7bae132aebe71ae7ab53".equals(hostID))
			return "cloud14";
		else if ("187698614e3a1473ed50b2c7ac6f70a0299f9af1e8c0066033ca6fbc".equals(hostID))
			return "cloud15";
		else if ("868c8c7b1e9aa9bceb00e0af9b1ed3b70dd34363d0e2b496919120a0".equals(hostID))
			return "cloud16";
		else if ("1eeca849baed74896b6af6e860e5f3f58bef9caf3f07b43e395491ba".equals(hostID))
			return "cloud17";
		else if ("01902033a6c07c7215dc2a969846543f9351ae8eaaa33241865bb89e".equals(hostID))
			return "cloud18";
		else 
			return hostID;
	}



}
