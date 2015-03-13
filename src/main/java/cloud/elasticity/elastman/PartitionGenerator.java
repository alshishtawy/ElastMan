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
import java.util.Random;

/**
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class PartitionGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int i = 1; i <= 60; i++) {
			calc(60, i);
			System.out.println("\n############################################\n");
		}
	}

	private static ArrayList<Integer> parts = null;
	
	@SuppressWarnings("unchecked")
	public static ArrayList<ArrayList<Integer>> calc(int part, int vms) {// number of total partitions and required number of VMs
		if(parts==null || parts.size() != part) {
//			System.out.println("Randomizing!!");
			parts = new ArrayList<Integer>(part);
			for (int i = 0; i < part; i++) {
				parts.add(i);
			}
			Random seed = new Random(643823);	// must use same seed for each exp
													// otherwise rebalance will shuffle all partitions around
			Collections.shuffle(parts,seed);
		}
		ArrayList<ArrayList<Integer>> current = new ArrayList<ArrayList<Integer>>();
		//		ArrayList<ArrayList<Integer>> next = new ArrayList<ArrayList<Integer>>();

		current.add((ArrayList<Integer>)parts.clone());

//		System.out.println(current);
		for (int j = 1; j < vms; j++) {
			int num = current.size();
			
//			double exactDonSize = ((double)part/(double)(num)) - ((double)part/(double)(num+1));
//			int idealDonSize = (int)exactDonSize;
					
			int idealSize = part/(num+1);
			double exactSize = (double)part/(double)(num+1);
			double blomb, blemb; // ha ha ha :) try figure that out after 6 months :P
			blomb = exactSize - idealSize;
			blemb = 0;
			ArrayList<Integer> child = new ArrayList<Integer>(); // the new child born form previous vms
			for (int i = 0; i < num; i++) {
				int size = current.get(i).size();
				
				
				int splitIndex = idealSize;
				blemb += blomb;
				while (blemb >= 0.5 && splitIndex > 0 && splitIndex<size) {
					splitIndex++;
					blemb--;
				}
				
				
				if(splitIndex > 0 && splitIndex<size) {					
					child.addAll(current.get(i).subList(splitIndex, size));
					current.get(i).removeAll(child);
				}
			}
			current.add(child);

		}


		int x = 0, c = 0;
		for (ArrayList<Integer> arrayList : current) {
			System.out.print(++c + ": " + arrayList.size() + ":\t");
			System.out.println(arrayList);
			x += arrayList.size();
		}
		System.out.println("Total: " + x );

		return current;
	}


}
