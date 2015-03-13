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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an implementation of {@link SyncServer} that uses a web server and PHP.
 * It can be accesed using HTTP protocol.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */

public class WebSyncServer implements SyncServer {
	
	static Logger log = LoggerFactory.getLogger(WebSyncServer.class);
		
	String serverUrl;
	
	public WebSyncServer(String serverUrl) {
		this.serverUrl = serverUrl;
	}
	
	void http(String key, String val) {
		HttpURLConnection con;
		try {
			String url = String.format("%s?%s=%s",serverUrl, key, URLEncoder.encode( val, "UTF-8"));
			log.debug("Connecting to VoldCache at {}", url);
			con = (HttpURLConnection) new URL(url).openConnection();
			String res;
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			log.debug("Result:");
			while ((res=in.readLine()) != null) {
				log.debug("\t{}", res);
			}
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void lock() {
		http("lock", "1");
	}

	public void unlock() {
		http("lock", "0");
	}

	public void reset() {
		http("reset", "1");
	}

	public void clusterCreate(String clusterConfig) {
		http("clusterCreate", clusterConfig);
		
	}

	public void clusterAppend(String clusterConfig) {
		http("cluster", clusterConfig);
		
	}

	public void log(String log) {
		http("log", log);
	}

}
