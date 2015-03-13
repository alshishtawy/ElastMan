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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.NovaAsyncApi;
import org.jclouds.openstack.nova.v2_0.domain.Address;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.Image;
import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.jclouds.openstack.nova.v2_0.domain.Server.Status;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.features.FlavorApi;
import org.jclouds.openstack.nova.v2_0.features.ImageApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;
import org.jclouds.rest.RestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;

/**
 * This class is a wrapper for the basic OpenStack Compute (Nova) operations
 * that are needed.  
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */

public class Nova {

	static Logger log = LoggerFactory.getLogger(Nova.class);

	private final String provider = "openstack-nova";

	private String username;
	private String password;
	private String keyName;
	private String zone;
	private String endpoint;
	private String userData = null;


	private ComputeService compute;
	private RestContext<NovaApi, NovaAsyncApi> nova;
	private Set<String> zones;



	/**
	 * The Constructor to initialize a Nova object.
	 * 
	 * @param username	A valid OpenStack username in the form Tenant:UserName
	 * @param password	The OpenStack password
	 * @param keyName	Name of SSH key to use
	 * @param zone		The OpenStack availability zone such as the default RegionOne
	 * @param endpoint	The URL to access OpenStack API such as http://192.168.1.1:5000/v2.0/
	 */
	public Nova(String username, String password, String keyName, String zone, String endpoint) {
		super();
		this.username = username;
		this.password = password;
		this.keyName = keyName;
		this.zone = zone;
		this.endpoint = endpoint;

		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				close();
				e.printStackTrace();
				System.exit(1);
			}
		});
	}




	/**
	 * Open a connection to the OpenStack API.
	 */
	public void open() {
		Iterable<Module> modules = ImmutableSet.<Module> of(
				//				new SshjSshClientModule(),  
				new SLF4JLoggingModule()//,
				//				new EnterpriseConfigurationModule()
				);

		ComputeServiceContext context = ContextBuilder.newBuilder(provider)
				.credentials(username, password)
				.endpoint(endpoint)
				.modules(modules)
				.buildView(ComputeServiceContext.class);

		compute = context.getComputeService();
		nova = context.unwrap();
		zones = nova.getApi().getConfiguredZones();	

		if(!zones.contains(this.zone)) {
			log.error("The zone {} is invalid!", this.zone);
		}
	}

	/**
	 * Close the connection to the OpenStack API
	 */
	public void close() {
		if(nova != null) {
			nova.close();
			nova = null;
		}

		if(compute != null) {
			compute.getContext().close();
			compute = null;
		}

	}


	/**
	 * Get a list of available VM flavors.
	 *  
	 * @return A list of available VM flavors.
	 */
	public Iterable<? extends Flavor> getFlavors() {
		if (nova == null) {
			log.error("nova object is not initialized! call open() first!");
			return null;
		}
		FlavorApi flavorApi = nova.getApi().getFlavorApiForZone(zone);
		return flavorApi.listInDetail().concat();

	}


	/**
	 * Get a list of available VM images.
	 *  
	 * @return A list of available VM images.
	 * 
	 */public Iterable<? extends Image> getImages() {
		 if (nova == null) {
			 log.error("nova object is not initialized! call open() first!");
			 return null;
		 }
		 ImageApi imageApi = nova.getApi().getImageApiForZone(zone);
		 return imageApi.listInDetail().concat();
	 }


	 /**
	  * Get a list of current user VMs in the cluster.
	  * 
	  * @return A list of user VMs
	  */
	 public Iterable<? extends Server> getVMs() {
		 if (nova == null) {
			 log.error("nova object is not initialized! call open() first!");
			 return null;
		 }
		 ServerApi serverApi = nova.getApi().getServerApiForZone(zone);
		 return serverApi.listInDetail().concat();
	 }


	 /**
	  * The name of the ssh key injected into the VMs
	  * @return
	  */
	 public String getKeyName() {
		 return keyName;
	 }

	 /**
	  * Terminated and deletes a VM
	  * 
	 * @param vmId The ID of the VM.
	 * @return	True if successful. False otherwise.
	 */
	public boolean deleteVM(String vmId) {
		 return nova.getApi().getServerApiForZone(zone).delete(vmId);
	 }

	 public synchronized void createVMs(int count, String prefix, int startID, String img, String flv, Runnable callback) {

		 final CyclicBarrier barrier = new CyclicBarrier(count, callback);
		 
		 for (int nextID = startID; nextID <startID+count; nextID++) {
			 
			 ServerCreated tmpServer = null;

			 try {
				 log.debug("Start creating server {}{}", prefix, nextID );
				 synchronized (nova) {
					 if(userData != null) {
						 tmpServer = nova.getApi().getServerApiForZone(zone).create(prefix + nextID, img, flv, CreateServerOptions.Builder.keyPairName(keyName).userData(userData.getBytes()));
					 } else {
						 tmpServer = nova.getApi().getServerApiForZone(zone).create(prefix + nextID, img, flv, CreateServerOptions.Builder.keyPairName(keyName));
					 }
				 }
				 log.debug("Successfully created server {}{}", prefix, nextID );
			 } catch (Exception e) {
				 log.error("ERROR creating server {}{}", prefix, nextID );
				 log.error(e.getMessage());
				 e.printStackTrace();
				 new Thread(){	// A Stupid solution for the barrier
					 public void run() {
						 try {
							barrier.await();
						} catch (Exception e) {
							e.printStackTrace();
						}
					 }
				 }.start();
				 continue;
			 }
			 
			 // FIXME: There is a rate limit in OpenStack that throws an error
			 // if more than 10 requests per minuts! Don't know where to change that!
			 // Because of that I sleep 5 seconds :)
			 try {
				 Thread.sleep(5000);
			 } catch (InterruptedException e) {
				 e.printStackTrace();
			 }


			 final Server newServer=nova.getApi().getServerApiForZone(zone).get(tmpServer.getId());

			 log.info("New server created: {}, {}, {}.", new Object[]{newServer.getId(), newServer.getName()});
			
			 
//			 // add it to web server log
//			 // TODO: This is not really needed
//			 sync.log("New server created: " + newServer.getId() + ", " + newServer.getName() );


			 new Thread(){
				 public void run() {
					 Status p=newServer.getStatus();
					 log.trace("Initial status is {}", p);

					 while(p != Status.ACTIVE){
						 try {
							 Thread.sleep(1000);
						 } catch (InterruptedException e) {
							 // TODO Auto-generated catch block
							 e.printStackTrace();
						 }

						 // we must get a new server object to get the current status
						 synchronized (nova) {	// FIXME: I don't remember why synchronized! maybe it is not needed anymore?
							 p=nova.getApi().getServerApiForZone(zone).get(newServer.getId()).getStatus();
						 }
						 log.trace("Updated server-object status is {}", p);

					 }

					 Server readyServer=null;
					 synchronized (nova) {
						 readyServer = nova.getApi().getServerApiForZone(zone).get(newServer.getId());
					 }
					 log.info("New server is ready: {}\t{}\t{}\t{}", new Object[] {readyServer.getId(), readyServer.getName(), getAddr(readyServer), readyServer.getKeyName()});
					 try {
							barrier.await();
						} catch (Exception e) {
							e.printStackTrace();
						}
				 }
			 }.start();
		 }
	 }
	 
	 
	 


	 
		/**
		 * The user data is a script that is passed to the VM at creation time to customize it.
		 * 
		 * @return The current script.
		 */
		public String getUserData() {
			return userData;
		}


		/**
		 * The user data is a script that is passed to the VM at creation time to customize it.
		 * 
		 * @param userData	The new script.
		 */
		public void setUserData(String userData) {
			this.userData = userData;
		}

		public static String getAddr(Server s) {
			try {
				return ((Address)(s.getAddresses().get("private").toArray()[0])).getAddr();
			} catch (Exception e) {
				return "x.x.x.x";
			}
		}
}
