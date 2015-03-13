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
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 * 
 * A Generic PID controller.
 *
 */
// FIXME: Inplelent Steppable
public class PIDController {

	static Logger log = LoggerFactory.getLogger(PIDController.class);

	/**
	 * Operating point for input.
	 * The feedback input to the controller, which is usually the
	 * filtered output of the controlled system.
	 */
	private double inOp;

	/**
	 * Operation point for output.
	 * The output signal of the controller, or control input from
	 * the system's point of view.
	 */
	private double outOp;

	/**
	 * The reference input or set point (the desired value for the measured output).
	 * The value is normalized using normalizedSetPoint = setpoint - inOp.
	 * The value is typically 0.
	 */
	private double normalizedSetPoint;

	/**
	 * The controller gains.
	 */
	private double kp, ki, kd;

	//	For internal calculations
	private double iTerm;
	private double prevError;
	private boolean firstStep;



	/**
	 * Creates a PID controller object.
	 * 
	 * @param inOp Operating point for the input (filtered output of the controlled system).
	 * @param outOp Operating point for the output.
	 * @param normalizedSetPoint The Desired value
	 * @param kp P controller gain.
	 * @param ki I controller gain.
	 * @param kd D  controller gain.
	 * 
	 */
	public PIDController(double inOp, double outOp, double normalizedSetPoint, double kp,
			double ki, double kd) {
		super();
		this.inOp = inOp;
		this.outOp = outOp;
		this.normalizedSetPoint = normalizedSetPoint;
		this.kp = kp;
		this.ki = ki;
		this.kd = kd;
		prevError=0;
		iTerm=0;
		firstStep=true;
	}




	
	
	// input: filtered p99, output: throughput per node
	/**
	 * Calculates the next control signal value. 
	 * 
	 * @param input Current measured (and filtered) system output without normalization.
	 * @return Next control value.
	 */
	public double step(double input) {


		double normalizedInput = input - inOp;  // normalize input using the operating point

		//		long now = System.nanoTime()/1000000000; // time now in seconds

		//		   long timeChange = (now - lastTime);
		//		   if(timeChange>=period)
		//		   {
		/*Compute all the working error variables*/
		double error = normalizedSetPoint - normalizedInput;

		// TODO: chenge these constants to setters & getters
		// limit the error
		if(error > 20000000) { // 20 micro second
			log.warn("WARNING!! Very large +ve error: {}", error);
			error=20000000;
		} else if(error < -20000000) {
			log.warn("WARNING!! Very large -ve error: {}", error);
			error=-20000000;
		}
		
		
		// TODO: check if we need to init prevError or just set it to 0
		if(firstStep){
			prevError = error;
			firstStep = false;
		}
		
		// Calculate the I term
		iTerm += error;

		//integrator windup
		if(iTerm > 10000000) {
			iTerm = 1500000;
			log.warn("WARNING!! Integrator windup +");
		} else if(iTerm < -10000000) {
			iTerm= -1500000;
			log.warn("WARNING!! Integrator windup -");
		}	

		// Calculate D term
		double dTerm = (error - prevError);

		/*Compute PID Output*/
		double output = kp*error + ki*iTerm + kd*dTerm;

		//		if(output > outMax) output = outMax;
		//		      else if(output < outMin) output = outMin;

		/*Remember some variables for next time*/
		//		      lastInput = input;
		prevError = error;
		//		      lastTime = now;
		//		   }

		// TODO: check the two PID controllers types. One relative and one abslute. Make a flage for it
		return outOp; // TODO: check if denormalize output
	}

	
	/**
	 * Used mainly when switching back to PID control. When the controller switches from PID to another controller, then
	 * later switches back to PID controller, it is better to reset to forget PID history because probably the situation is 
	 * different now hand the old history does not make sense anymore.
	 * 
	 * @param input	The current input signal value
	 */
	public void reset() {
		// prevError = normalizedSetPoint - (input - inOp);
		prevError = 0;
		iTerm=0;
		firstStep=true;
	}
}
