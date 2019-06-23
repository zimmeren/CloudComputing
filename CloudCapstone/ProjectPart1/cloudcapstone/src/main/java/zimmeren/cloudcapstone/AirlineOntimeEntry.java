package zimmeren.cloudcapstone;

import java.util.StringTokenizer;

//Year,Month,DayofMonth,UniqueCarrier,FlightNum,Origin,Dest,CRSDepTime,DepDelay,ArrDelay
//1995,5,    4,         US,           1277,     PVD,   PHL, 1225,      -1.00,   -8.00

public class AirlineOntimeEntry {
	int year;
	int month;
	int day;
	String carrier;
	String flightNum;
	String origin;
	String dest;
	int depTime;
	float depDelay;
	float arrDelay;
	boolean valid;
	
	public AirlineOntimeEntry() {
		this.valid = false;
	}
	
	public void parseEntry(String entry) {
		StringTokenizer itr = new StringTokenizer(entry, ",");
		int index = 0;
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			try {
				switch (index) {
					case 0:
						if (token.equals("Year")) {
							//this is the header line
							return;
						}
						this.year = Integer.parseInt(token);
						break;
					case 1:
						this.month = Integer.parseInt(token);
						break;
					case 2:
						this.day = Integer.parseInt(token);
						break;
					case 3:
						this.carrier = token;
						break;
					case 4:
						this.flightNum = token;
						break;
					case 5:
						this.origin = token;
						break;
					case 6:
						this.dest = token;
						break;
					case 7:
						this.depTime = Integer.parseInt(token);
						break;
					case 8:
						this.depDelay = Float.parseFloat(token);
						break;
					case 9:
						this.arrDelay = Float.parseFloat(token);
						this.valid = true;
						break;
					default:
						System.out.println("Airline Entry too long");
				}
			}
			catch (Exception e) {
				System.out.println("parse failure at index: " + index);
				System.out.println(entry);
				System.out.println(e.toString());
			}
			index++;
		}
	}
	
	public String toString() {
		return "Year: " + year + " Month: " + month + " DayofMonth: " + day + 
				" UniqueCarrier: " + carrier + " FlightNum: " + flightNum + 
				" Origin: " + origin + " Dest: " + dest + " CRSDepTime: " +
				depTime + " DepDelay: " + depDelay + "ArrDelay: " + arrDelay;
	}
}
