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
 * 
 * 
 * @author Ahmad Al-Shishtawy <ahmadas@kth.se>
 *
 */
public class SimpleBinaryClassifier {

	
	/// x is read throughput
	/// Y is write throughput
	/// line defined by (x1,y1) and (x2,y2) is our model we got from identification
	
	/// current value is (x4,y4)
	///	desired value is at the intersection between (0,0)-(x4,y4) and model line
	
	public static void main(String[] args) {
		SimpleBinaryClassifier c = new SimpleBinaryClassifier(1800, 200, 0, 1000);
		double out = c.classify(95.0, 5.0);
		System.out.println(out);
		 out = c.classify(90.0, 10.0);
		System.out.println(out);
		out = c.classify(80.0, 20.0);
		System.out.println(out);
		out = c.classify(50.0, 50.0);
		System.out.println(out);
		out = c.classify(30.0, 70.0);
		System.out.println(out);
		
		
	}
	
	private final Point p1, p2, o; // line describing the model, o is the origin
	
	public SimpleBinaryClassifier(double x1, double y1, double x2, double y2) {
		p1  = new Point(x1, y1);
		p2  = new Point(x2, y2);
		o = new Point(0, 0);
	}
	
	public double classify(Double readTP, Double writeTP) {
		Point p = intersection(p1, p2, o, new Point(readTP, writeTP));
		System.out.println("Intersect at " + p);
		return p.getX()+p.getY();  // return the optimal total throughput
	}

	private class Point {
		double x,y;
		public Point(double x, double y) {
			this.x=x;
			this.y=y;
		}
		public double getX() {
			return x;
		}
		public double getY() {
			return y;
		}
		@Override
		public String toString() {
			
			return "("+x+","+y+")";
		}
	}

	public Point intersection(Point p1, Point p2, Point p3, Point p4) {
		return intersection(p1.getX(), p1.getY(), p2.getX(), p2.getY(), p3.getX(), p3.getY(), p4.getX(), p4.getY());
	}
	
	/**
	 * Computes the doubleersection between two lines. The calculated Point is approximate, 
	 * since doubleegers are used. If you need a more precise result, use doubles
	 * everywhere. 
	 * (c) 2007 Alexander Hristov. Use Freely (LGPL license). http://www.ahristov.com
	 *
	 * @param x1 Point 1 of Line 1
	 * @param y1 Point 1 of Line 1
	 * @param x2 Point 2 of Line 1
	 * @param y2 Point 2 of Line 1
	 * @param x3 Point 1 of Line 2
	 * @param y3 Point 1 of Line 2
	 * @param x4 Point 2 of Line 2
	 * @param y4 Point 2 of Line 2
	 * @return Point where the segments doubleersect, or null if they don't
	 */

	public Point intersection(

			double x1,double y1,double x2,double y2, 

			double x3, double y3, double x4,double y4

			) {

		double d = (x1-x2)*(y3-y4) - (y1-y2)*(x3-x4);

		if (d == 0) return null;



		double xi = ((x3-x4)*(x1*y2-y1*x2)-(x1-x2)*(x3*y4-y3*x4))/d;

		double yi = ((y3-y4)*(x1*y2-y1*x2)-(y1-y2)*(x3*y4-y3*x4))/d;



		return new Point(xi,yi);

	}
	

}
