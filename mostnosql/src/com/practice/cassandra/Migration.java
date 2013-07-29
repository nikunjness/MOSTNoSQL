package com.practice.cassandra;
import java.sql.*;
public class Migration {
	public Connection con=null;
	void setConnection()
	{
		try {
			con=getConnection("jdbc:mysql://localhost:3306/most", "root", "nix123");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	boolean isConnectionSet()
	{
		return (con!= null)?true:false;
	}
	private Connection getConnection(String dbPath,String username,String password) throws SQLException
	{
		
		try {
				Class.forName("com.mysql.jdbc.Driver");
		 		con = DriverManager.getConnection(dbPath,username,password);
		 		
			}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return con;
	}
	ResultSet getAllDatapoints()
	{
		if(!isConnectionSet())
		setConnection();	
		PreparedStatement prest;
		ResultSet rs1=null;
		try{
		  	String sql = "SELECT distinct datapoint_name FROM data";
		  	prest = con.prepareStatement(sql);
		  	//prest.setString(1,"tem1");
		  	//System.out.println(prest);
		  	rs1= prest.executeQuery();  				  	
		  	
	  }
	  catch (SQLException s)
	  {
		  System.out.println(s);
		  System.out.println("SQL statement is not executed!");
	  }
		return rs1;
	}
	ResultSet getDataFromDatapoint(String dpname)
	{
		if(!isConnectionSet())
		setConnection();	
		PreparedStatement prest;
		ResultSet rs1=null;
		try{
		  	String sql = "SELECT * FROM data where datapoint_name=?";
		  	prest = con.prepareStatement(sql);
		  	prest.setString(1,dpname);
		  	//System.out.println(prest);
		  	rs1= prest.executeQuery();  				  	
		  	
	  }
	  catch (SQLException s)
	  {
		  System.out.println(s);
		  System.out.println("SQL statement is not executed!");
	  }
		return rs1;
	}
}
