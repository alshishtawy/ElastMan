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

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PublicKey;
import java.util.Scanner;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.Image;
import org.jclouds.openstack.nova.v2_0.domain.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.jclouds.compute.ComputeServiceContext;
//import org.jclouds.PropertiesBuilder;


/**
 * This is the main ElastMan program. It is used to initialize the Voldemort
 * cluster, run system identification, test the controller under various workloads,
 * and run the controller in production.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class App 
{
	static Logger log = LoggerFactory.getLogger(App.class);
	
	

	private Cluster cluster;
	private Nova nova;
	
	/**
	 * The {@link SyncServer} is used to make sure
	 * that all Voldemort servers have the same cluster.xml file.
	 * <p>
	 * The SyncServer is used when creating new Voldemort VMs.
	 * It is also optionally used to keep a log of what is happening
	 * in the cluster.
	 * 
	 * @see SyncServer
	 */
	private SyncServer sync;
	
	
	/**
	 * Parsed command line arguments used to configure ElastMan.
	 */
	private static CommandLine cmd = null;

	
	/**
	 * The constructor.
	 */
	public App() {
		
		
	}

	private static Scanner scanner = new Scanner(System.in);

	
	/**
	 * The entry point to the ElastMan main program.
	 * 
	 * @param args	The first argument is the mode which can be inter, ident, or control
	 * corresponding to interactive mode, system identification mode, or control mode.
	 * The second argument is the configuration file
	 * The third argument is password password  
	 */
	public static void main( String[] args )
	{
		// 1) parse the command line
		// For more information http://commons.apache.org/cli/
		Options options = new Options();
		options.addOption("i", "ident",  false, "Enter system identification mode.");
		options.addOption("c", "control",  false, "Enter controller mode.");
		options.addOption("o", "options", true, "Configuration file. Default elastman.conf");
		options.addOption("u", "username", true, "Username in the form Tenant:UserName");
		options.addOption("p", "password", true, "User password");
		options.addOption("k", "keyname", true, "Name of SSH key to use");
		options.addOption("z", "zone", true, "The OpenStack availability zone such as the default RegionOne");
		options.addOption("e", "endpoint", true, "The URL to access OpenStack API such as http://192.168.1.1:5000/v2.0/");
		options.addOption("s", "syncserver", true, "The URL access the WebSyncServer");
		options.addOption("h", "help", false, "Print this help");
		
		CommandLineParser parser = new GnuParser();
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e2) {
			System.out.println(e2.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "ElastMan" , options, true );
			System.exit(1);
		}
		
		// if h then show help and exit
		if(cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "ElastMan" , options, true );
			System.exit(0);
		}
		
		// 2) Try to load the properties file.
		// Command line arguments override settings in the properties file
		// If no properties file exists. defaults will be used
		
		String filename = "control.prop"; // the default file name
		if(cmd.hasOption("o")) {
			filename = cmd.getOptionValue("o");
		}
		Props.load(filename, cmd);
//		Props.save(filename);
//		System.exit(-1);
		
		// 3) If no password in command line nor in config file then ask the user
		if(Props.password == null) {
			Console cons;
			char[] passwd;
			if ((cons = System.console()) != null &&
					// more secure and without echo!
					(passwd = cons.readPassword("[%s]", "Password:")) != null) {
				Props.password = new String(passwd);
			} else {
				// if you don't have a console! E.g., Running in eclipse
				System.out.print("Password: ");
				Props.password = scanner.nextLine();
			}
		}
	
		// 4) Start the UI
		App app = new App();
		app.textUI(args);
		
	}
	
	private void textUI(String[] args) {
		
		System.out.println("Welcome to ElastMan (v0.0.9)");
		
		System.out.print("Starting Nova Client...");
		nova = new Nova(Props.username, Props.password, Props.keyname, Props.zone, Props.endpoint);
		sync = new WebSyncServer(Props.webSyncServer);
		cluster = new Cluster(nova, sync);
		cluster.setVoldPrefix(Props.voldPrefix);
		cluster.setVoldImage(Props.voldImage);
		cluster.setVoldFlavor(Props.voldFlavor);
		cluster.setReplicationFactor(Props.voldReplicationFactor);
		cluster.setYcsbPrefix(Props.ycsbPrefix);
		cluster.setYcsbImage(Props.ycsbImage);
		cluster.setYcsbFlavor(Props.ycsbFlavor);
		
		nova.open();
		System.out.println("OK");

		if(cmd.hasOption("i")) { // enter identification mode
			cluster.updateVMs();
			new ElastManServer(false,cluster).run(); // start in this thread instead of .start();
			System.exit(0); // don't go to interactive mode
		}

		if(cmd.hasOption("c")) { // enter identification mode
			cluster.updateVMs();
			new ElastManServer(true,cluster).run(); // start in this thread instead of .start();
			System.exit(0); // don't go to interactive mode
		}

		int command;

		while(true) {
			System.out.println("\n0 - Exit\t1 - Status\t2 - Delete all\t3 - Images\t4 - flavors\t5 - create\t6 - Generate Cluster.xml\t7 - Identify\t8 - control");
			System.out.print("command > ");

			if((command = getInt()) == -1) continue;

			switch (command) {
			case 0:
				nova.close();
				System.out.println("Good bye!");
				System.exit(0);
				break;

			case 1:
				System.out.println("1 - All our VMs\t2 - Voldemort \t3 - YCSB");
				System.out.print("command > ");

				if((command = getInt()) == -1) break;

				switch (command) {
				case 1:
					cluster.updateVMs();
					showStatus(null);
					break;
				case 2:
					cluster.updateVMs();
					showStatus(cluster.getVoldPrefix());
					break;
				case 3:
					cluster.updateVMs();
					showStatus(cluster.getYcsbPrefix());
					break;
				default:
					System.out.println("Don't know this command yet! Try again later :P");
					break;
				}
				break;
			case 2:
				//				System.out.print("Are you sure? (y/n)? ");
				//				if(scanner.next().toLowerCase().equals("y")) {
				//					deleteVMs();
				//				}
				System.out.print("Enter prefix ( * for all ) > ");
				String prefix = scanner.next();
				if (prefix.equals("*")) {
					cluster.deleteVMs(null);
				} else {
					cluster.deleteVMs(prefix);
				}
				break;
			case 3:
				for (Image image: nova.getImages()) {
					System.out.println("\tID: " + image.getId() + "\tName: " + image.getName());
				}
				break;
			case 4:
				for (Flavor flavor: nova.getFlavors()) {
					System.out.println("\tID: " + flavor.getId() + "\tName: " + flavor.getName());
				}
				break;
			case 5:
				int count;
				System.out.print("Please enter: count prefix image_ID flavor_ID: ");
				count = getInt();
				cluster.createVMs(count, scanner.next(), scanner.next(), scanner.next());
				System.out.println("Done!");
				break;
			case 6:
				int nodes;
				System.out.print("Enter number of VMs in cluster, -1 for all: ");
				nodes = getInt();
				sync.reset();
				cluster.updateVMs();
				cluster.genCluster(nodes);
				sync.unlock();
				break;
			case 7:
				new ElastManServer(false,cluster).start();
				break;
			case 8:
				new ElastManServer(true,cluster).start();
				break;
			case 9:
				break;
			case 10:
				SSHClient ssh = new SSHClient();
				try {
					HostKeyVerifier hkv = new HostKeyVerifier() {

						//@Override
						public boolean verify(String hostname, int port, PublicKey key) {
							System.out.println(hostname + " " + port + " " + key); 
							return true;
						}
					}; // this ignores known_hosts and accepts any host. Less secure!

					ssh.addHostKeyVerifier(hkv);
					ssh.connect("cloud2.sics.se");
					//					ssh.authPassword("ahmad", "xxx");
					String username = "ahmad";
					File privateKey = new File("C:\\Users\\Admin\\Documents\\MyDocs\\Misc\\Ahmad\\ahmad");
					KeyProvider keys = ssh.loadKeys(privateKey.getPath());
					ssh.authPublickey(username, keys);
					final Session session = ssh.startSession();
					Command cmd = session.exec("date; hostname");
					BufferedReader in = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
					String res;
					while((res = in.readLine()) != null) {
						System.out.println(res);
					}
					// SSHClient client = new SSHClient();
					//					String username = "johndoe";
					//					File privateKey = new File("~/.ssh/id_rsa");
					//					KeyProvider keys = client.loadKeys(privateKey.getPath());
					//					client.authPublicKey(username, keys);

				} catch (UserAuthException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TransportException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			default:
				System.out.println("Don't know this command yet! Try again later :P");
				break;
			}
		}
	}

	

	private  static int getInt() {

		try {
			return scanner.nextInt();
		} catch (Exception e) {
			System.out.println("Invalid Input!");
			scanner.nextLine();
			return -1;
		}
	}


	///////////////////////////////////////////




	private void showStatus(String namePrefix) {

		int hosts=0, vms=0;
		boolean print = true;	// to print host name only if VMs found on it
		System.out.println("===========================");
		for (String hostId : cluster.getHosts()) {
			for(Server s : cluster.getVMsOnHost(hostId)) {
				if(namePrefix == null || s.getName().startsWith(namePrefix)) {
					if(print) {
						hosts++;
						System.out.println(Cluster.getHostName(hostId));
						print = false;
					}
					System.out.println(s.getName() + "\t" + Nova.getAddr(s) + "\t" + s.getId());
					vms++;
				}
			}
			if(!print) {	// this means that we printed something so we put a line to separate hosts and reset print
				print=true;
				System.out.println("===========================");
			}
		}
		System.out.println("found " + vms + " VMs running on " + hosts + " hosts!");


	}






}
