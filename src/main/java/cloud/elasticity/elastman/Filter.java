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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A smoothing (moving average) filter.
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class Filter {
	static Logger log = LoggerFactory.getLogger(Filter.class);

	private boolean firstInput;
	private double lastFilter = 0;
	private double alpha = 0.4;
	//	private boolean firstLargeValue = false;  // to ignore very large values only once



	/**
	 * Constructs a new moving average Filter Object with a precified alpha
	 * 
	 * @param alpha	The alpha value to use with the moving average filter
	 */
	public Filter(double alpha) {
		super();
		this.alpha = alpha;
		firstInput = true;
		lastFilter = 0;
	}	
	
	/**
	 * Resets the filter by forgetting the history.
	 */
	public void reset() {
		firstInput = true;
	}

	/**
	 * Get the current value of the filter.
	 * 
	 * @return Current filter vlaue. 
	 */
	public double getValue() {
		return lastFilter;
	}
	
	/**
	 * Calculates a smoothed version of the input signal.
	 * 
	 * @param input	The original signal.
	 * @return	The smoothed signal.
	 */
	public double step(double input) {

//		if(isRebalancing()) {
//			firstLargeValue=false;
//			lastFilter = Math.min(lastFilter, input);
//			System.out.println("Filter is ignoring current value because of rebalancing! " + input + " and returning " + lastFilter);
//		} else if((input-lastFilter)/lastFilter >= 0.3 && !firstLargeValue) { // FIXME: should be ABS value
//			firstLargeValue = true;
//			lastFilter = Math.min(lastFilter, input);  // should always be input
//			System.out.println("Filter is ignoring large value " + input + " and returning " + lastFilter);
//			// don't change lastFilter and return it as is
//		} else { // not large value or I get again a large value
//			lastFilter = (lastFilter*alpha) + (input * (1-alpha));
//			System.out.println("Filter says at timeStep " + timeStep + " : " +  Math.min(lastFilter, input)); 
//			firstLargeValue=false;
//		}
//		return Math.min(lastFilter, input);
//	}

		double f;
		if(firstInput) {
			firstInput = false;
			f = input;
		} else {
			f = (lastFilter*alpha) + (input * (1-alpha));
		}
		lastFilter = f;
		return f; 
	}
	
}







