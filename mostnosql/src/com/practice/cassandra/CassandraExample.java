package com.practice.cassandra;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraExample {
	
	public static void main(String[] args) {
		CassandraWrapper cw=new CassandraWrapper();
		try {
			cw.setEnvironment();
		} catch (InvalidRequestException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		/*Migration mig=new Migration();
		mig.setConnection();
		
		ResultSet rs=mig.getAllDatapoints();
		try {
			while (rs.next())
			{
					String datapoint_name = rs.getString(1);
					/*Date dt=rs.getDate(2);
					double value=rs.getDouble(3);*/
					/*System.out.println("\n\nDatapoint Name => "+datapoint_name);// + "\t- " + dt+ "\t- " + value);
					cw.createColumnfamily(datapoint_name);
					ResultSet rs1=mig.getDataFromDatapoint(datapoint_name);
					try {
						while (rs1.next())
						{
								//String dpname = rs1.getString(1);
								Date d=rs1.getDate(2);
								java.sql.Timestamp dt=rs1.getTimestamp(2);
								double value=rs1.getDouble(3);
								cw.insertData(datapoint_name,d, dt, value);
								System.out.println(datapoint_name + "\t- " + dt+ "\t- " + value);
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		cw.readData();
		
		
		
		
	}
	
	
}
