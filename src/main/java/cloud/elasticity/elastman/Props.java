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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class Props {
	
	static Logger log = LoggerFactory.getLogger(Props.class);
	
	public static Properties properties = new Properties();

	public static String username;
	public static String password; 
	public static String keyname; 
	public static String zone; 
	public static String endpoint; 
	public static String webSyncServer; 
	

	public static int server_port;
	public static int ident_client_min;
	public static int ident_client_max;
	public static int ident_client_delta;
	public static int ident_client_delay;
	public static int ident_period;
	public static int ident_sampling;
	public static boolean ident_client_manual;
	public static int control_interval;
	public static int voldCount;	// actual number of VoldVMs participating in the store according to the cluster.xml
	public static boolean createVMs;
	public static int voldMax;
	public static int voldMin;
	public static int voldDeltaMax;

	
	public static double control_kp;
	public static double control_ki;
	public static double control_kd;
	public static double control_inOp;
	public static double control_outOp;
	public static double filter_alpha;
	public static int control_warmup;
	public static double control_dead;
	public static double control_ff_throughputDelta;
	public static double control_ffr1;
	public static double control_ffw1;
	public static double control_ffr2;
	public static double control_ffw2;
	
	public static String voldPrefix;
	public static String voldImage;
	public static String voldFlavor;
	public static int voldReplicationFactor;
	public static String ycsbPrefix;
	public static String ycsbImage;
	public static String ycsbFlavor;
	
	

	public static void load(String filename, CommandLine cmd){
		FileInputStream propFile;
		
		
		try {
			propFile = new FileInputStream(filename);
			properties = new Properties(System.getProperties());
			properties.load(propFile);
			propFile.close();
		} catch (FileNotFoundException e1) {
			log.warn("Properties file not found!! Using defaults");
//			e1.printStackTrace();
		} catch (IOException e) {
			log.error("Properties file I/O error!! using defaults");
//			e.printStackTrace();
		}
		
		
		
		// TODO: make text constants to avoid typo bugs
		if(cmd.hasOption("u")) {
			properties.setProperty("username", cmd.getOptionValue("u"));
		}
		if(cmd.hasOption("p")) {
			properties.setProperty("password", cmd.getOptionValue("p"));
		}
		if(cmd.hasOption("k")) {
			properties.setProperty("keyname", cmd.getOptionValue("k"));
		}
		if(cmd.hasOption("z")) {
			properties.setProperty("zone", cmd.getOptionValue("z"));
		}
		if(cmd.hasOption("e")) {
			properties.setProperty("endpoint", cmd.getOptionValue("e"));
		}
		if(cmd.hasOption("s")) {
			properties.setProperty("webSyncServer", cmd.getOptionValue("s"));
		}
		
		username = properties.getProperty("username", "OpenstackDemo:user");
		password = properties.getProperty("password");
		keyname = properties.getProperty("keyname", "mykey");
		zone = properties.getProperty("zone", "RegionOne");
		endpoint = properties.getProperty("endpoint", "http://192.168.1.1:5000/v2.0/");
		webSyncServer = properties.getProperty("webSyncServer", "http://192.168.1.1:8080/cloud/voldcache.php");
		
		
		server_port = Integer.parseInt(properties.getProperty("server.port","4444"));
		ident_client_min = Integer.parseInt(properties.getProperty("ident.client.min","1"));
		ident_client_max = Integer.parseInt(properties.getProperty("ident.client.max","18"));
		ident_client_delta = Integer.parseInt(properties.getProperty("ident.client.delta","1"));
		ident_client_delay = Integer.parseInt(properties.getProperty("ident.client.delay","900"));
		ident_period = Integer.parseInt(properties.getProperty("ident.period","4"));
		ident_sampling = Integer.parseInt(properties.getProperty("ident.sampling","300"));
		ident_client_manual = Boolean.parseBoolean(properties.getProperty("ident.client.manual","false"));

		control_interval = Integer.parseInt(properties.getProperty("control.interval","300"));
		
		voldCount = Integer.parseInt(properties.getProperty("cloud.voldVMs","0"));
		createVMs = Boolean.parseBoolean(properties.getProperty("act.createVMs","true"));
		voldMax = Integer.parseInt(properties.getProperty("act.voldMax","27"));
		voldMin = Integer.parseInt(properties.getProperty("act.voldMin","3"));
		voldDeltaMax = Integer.parseInt(properties.getProperty("act.voldDeltaMax","7"));
		
		
		control_kp=Double.parseDouble(properties.getProperty("control.kp","0"));
		control_ki=Double.parseDouble(properties.getProperty("control.ki","0"));
		control_kd=Double.parseDouble(properties.getProperty("control.kd","0"));
			
		control_inOp = Double.parseDouble(properties.getProperty("control.inOp","6000000"));
		control_outOp = Double.parseDouble(properties.getProperty("control.outOp","1400"));

		filter_alpha = Double.parseDouble(properties.getProperty("filter.alpha","0.4"));
		control_warmup = Integer.parseInt(properties.getProperty("control.warmup","4"));
		control_dead = Double.parseDouble(properties.getProperty("control.dead","500000"));
		control_ff_throughputDelta = Double.parseDouble(properties.getProperty("control.ff.throughputDelta","1400"));
		
		control_ffr1 = Double.parseDouble(properties.getProperty("control.ff.r1","1980"));
		control_ffw1 = Double.parseDouble(properties.getProperty("control.ff.w1","220"));
		control_ffr2 = Double.parseDouble(properties.getProperty("control.ff.r2","0"));
		control_ffw2 = Double.parseDouble(properties.getProperty("control.ff.w2","1000"));
		
		
		voldPrefix = properties.getProperty("voldPrefix","vold");
		voldImage = properties.getProperty("voldImage");
		voldFlavor = properties.getProperty("voldFlavor","3");
		voldReplicationFactor = Integer.parseInt(properties.getProperty("voldReplicationFactor","3"));
		ycsbPrefix = properties.getProperty("ycsbPrefix","ycsb");
		ycsbImage = properties.getProperty("ycsbImage");
		ycsbFlavor = properties.getProperty("ycsbFlavor","3");
				
	}
	
	public static void save(String filename) {
		properties.setProperty("username", username);
		properties.setProperty("password", password);
		properties.setProperty("keyname", keyname);
		properties.setProperty("zone", zone);
		properties.setProperty("endpoint", endpoint);
		properties.setProperty("webSyncServer", webSyncServer);
		properties.setProperty("server.port", ""+server_port);
		properties.setProperty("ident.client.min", ""+ident_client_min);
		properties.setProperty("ident.client.max", ""+ident_client_max);
		properties.setProperty("ident.client.delta", ""+ident_client_delta);
		properties.setProperty("ident.client.delay", ""+ident_client_delay);
		properties.setProperty("ident.period", ""+ident_period);
		properties.setProperty("ident.sampling", ""+ident_sampling);
		properties.setProperty("ident.client.manual", ""+ident_client_manual);
		properties.setProperty("control.interval", ""+control_interval);
		properties.setProperty("cloud.voldVMs", ""+voldCount);
		properties.setProperty("act.createVMs", ""+createVMs);
		properties.setProperty("act.voldMax", ""+voldMax);
		properties.setProperty("act.voldMin", ""+voldMin);
		properties.setProperty("act.voldDeltaMax", ""+voldDeltaMax);


		properties.setProperty("control.kp", ""+control_kp);
		properties.setProperty("control.ki", ""+control_ki);
		properties.setProperty("control.kd", ""+control_kd);
		
		properties.setProperty("control.inOp", ""+control_inOp);
		properties.setProperty("control.outOp", ""+control_outOp);
		
		properties.setProperty("filter.alpha", ""+filter_alpha);
		properties.setProperty("control.warmup", ""+control_warmup);
		properties.setProperty("control.dead", ""+control_dead);
		properties.setProperty("control.ff.throughputDelta", ""+control_ff_throughputDelta);
		
		properties.setProperty("control.ff.r1", ""+control_ffr1);
		properties.setProperty("control.ff.w1", ""+control_ffw1);
		properties.setProperty("control.ff.r2", ""+control_ffr2);
		properties.setProperty("control.ff.w2", ""+control_ffw2);
		
		
		
		properties.setProperty("voldPrefix", voldPrefix);
		properties.setProperty("voldImage", voldImage);
		properties.setProperty("voldFlavor", voldFlavor);
		properties.setProperty("voldReplicationFactor", ""+voldReplicationFactor);
		properties.setProperty("ycsbPrefix", ycsbPrefix);
		properties.setProperty("ycsbImage", ycsbImage);
		properties.setProperty("ycsbFlavor", ycsbFlavor);

//		properties.setProperty(, ""+);
//		properties.setProperty(, ""+);
//		properties.setProperty(, ""+);
//		properties.setProperty(, ""+);
//		properties.setProperty(, ""+);

		
	

		FileOutputStream propFile;
		try {
			propFile = new FileOutputStream(filename);
			properties.store(propFile, "ElastMan Configurations File");
		} catch (FileNotFoundException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
	}

}
