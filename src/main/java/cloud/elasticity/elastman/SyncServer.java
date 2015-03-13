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
 * The SyncServer is used to make sure
 * that all Voldemort servers are in sync, i.e., they have the same cluster.xml file.
 * <p>
 * The SyncServer is typically used when creating new Voldemort VMs.
 * The typical use of a SyncServer is 1) lock the server. 2) start creating Voldemort VMs.
 * 3) when all VMs are ready, use their IP address to create a new cluster.xml file.
 * 4) unlock the SyncServer to allow the new VMs to download the cluster.xml file.
 * <p>
 * The SyncServer requires a script or process on the new Voldemort VMs that check and wait
 * till the server is unlocked, then download cluster.xml and starts the Voldemort server.
 * <p>
 * SyncServer can also be used to keep a log of what is happening both by the controller
 * created the VMs and/or by the script or process that runs on the new VMs.
 * in the cluster.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */

public interface SyncServer {
	public void lock();
	
	public void unlock();
	
	public void reset();
	
	public void clusterCreate(String clusterConfig);
	
	public void clusterAppend(String clusterConfig);
	
	public void log(String log);
	

}
