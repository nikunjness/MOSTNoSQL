package com.practice.cassandra;
import java.sql.*;
public class Connectivity {

	/**
	 * Author : Nikunj Thakkar
	 * Mail to: 	nikunjthakkar1992@gmail.com
	 * @param args
	 */
	public static void main(String[] args) {
	
		  Connection con = null;
		  PreparedStatement prest;
		  try{
			  Class.forName("com.mysql.jdbc.Driver");
			  con = DriverManager.getConnection("jdbc:mysql://localhost:3306/most","root","nix123");
			  try{
				  	String sql = "SELECT count(*) as 'Datapoint_Name' FROM data WHERE datapoint_name =?";
				  	prest = con.prepareStatement(sql);
				  	prest.setString(1,"tem1");
				  	System.out.println(prest);
				  	ResultSet rs1 = prest.executeQuery();  				  	
				  	while (rs1.next())
				  	{
				  			String datapoint_name = rs1.getString(1);
				  			/*Date dt=rs1.getDate(2);
				  			double value=rs1.getDouble(3); */
				  			System.out.println(datapoint_name);// + "\t- " + dt+ "\t- " + value);
				  	}
			  }
			  catch (SQLException s)
			  {
				  System.out.println(s);
				  System.out.println("SQL statement is not executed!");
			  }
		 }
		 catch (Exception e)
		 {
			 	e.printStackTrace();
		 }
	}
}
