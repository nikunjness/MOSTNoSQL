package com.practice.cassandra;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;


import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class CassandraWrapper {
	public Cluster myCluster=null;
	public static String keyspaceName= "mostkeyspace";
	public static String cfdatapoint="datapoint";
	public String cfcreate=null;
	public KeyspaceDefinition ksdef=null;
	public ColumnFamilyDefinition cfdef=null;
	public static Keyspace keyspace=null;
	private static final ColumnSliceIterator.ColumnSliceFinish<UUID> FINISH = new ColumnSliceIterator.ColumnSliceFinish<UUID>() {

@Override
public UUID function() {
return TimeUUIDUtils.getUniqueTimeUUIDinMillis();
}
};
	void setEnvironment() throws InvalidRequestException,TException
	{
		try
		{
			myCluster = HFactory.getOrCreateCluster("Test Sample","localhost:9160");
			ksdef=myCluster.describeKeyspace(keyspaceName);
			if(ksdef==null)
			addKeyspacetoCassandra();		
			keyspace=HFactory.createKeyspace("mostkeyspace", myCluster);
		}
		catch(Exception e)
		{
			System.out.print("Unalble to setup environment");
			e.printStackTrace();
		}
	}
	void addKeyspacetoCassandra() throws InvalidRequestException,TException
	{
		try
		{
			ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,cfdatapoint,ComparatorType.BYTESTYPE);
			int replicationFactor=1;
			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,ThriftKsDef.DEF_STRATEGY_CLASS,replicationFactor,Arrays.asList(cfDef));
			//Add the schema to the cluster.
			myCluster.addKeyspace(newKeyspace);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	void dropkeyspace()
	{
		myCluster.dropColumnFamily(keyspaceName, cfdatapoint);
		myCluster.dropKeyspace(keyspaceName,true);
	}
	void createColumnfamily(String cfname)
	{
		
		if(cfname.equals("") || cfname.trim().equals(null))
		{
			return;
		}
		else
		{
			cfcreate= cfname.toLowerCase();
			if(checkExist(cfcreate)==false)
			{
				cfdef=HFactory.createColumnFamilyDefinition(keyspaceName, cfcreate);
				myCluster.addColumnFamily(cfdef, true);
			}
			
		}
	}
	boolean checkExist(String cfname)
	{	
		List<ColumnFamilyDefinition> lcf=ksdef.getCfDefs();		
		Iterator<ColumnFamilyDefinition> it=lcf.iterator();		
		while(it.hasNext())
		{			
			ColumnFamilyDefinition cf=it.next();
			if(cf.getName().equals(cfname))
			return true;
			
		}
		return false;
	}
	ColumnFamilyDefinition getCfdf(String cfname)
	{	
		cfname=cfname.toLowerCase();
		List<ColumnFamilyDefinition> lcf=ksdef.getCfDefs();		
		Iterator<ColumnFamilyDefinition> it=lcf.iterator();		
		while(it.hasNext())
		{
			ColumnFamilyDefinition cf=it.next();
			if(cf.getName().equals(cfname))
			return cf;
			
		}
		return null;
	}
	void insertData(String dpname,Date d,Timestamp u,double value)
	{
		try
		{
			long timeInMicroSeconds=u.getTime();
			//UUID timeUUIDColumnName = TimeUUIDUtils.getTimeUUID(timeInMicroSeconds);
			System.out.println(d+"\t"+timeInMicroSeconds+"\t"+value);
			DateSerializer ds=new DateSerializer();
			Mutator<Date> mu=HFactory.createMutator(keyspace, ds);
			mu.insert(d, dpname, HFactory.createColumn(timeInMicroSeconds, value));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	void setColumnfamilyDefination(String cfname)
	{
		if(checkExist(cfname)==false)
		createColumnfamily(cfname);
		cfdef=getCfdf(cfname);
		
	}
	void readData()
	{
		int row_count = 100;
		System.out.println("\n\n Readind data from con1 table =>\n");
		DateSerializer dt=new DateSerializer();
		LongSerializer ls=new LongSerializer();
		//UUIDSerializer ud=new UUIDSerializer();
		DoubleSerializer ds=new DoubleSerializer();
		RangeSlicesQuery<Date, Long, Double> sl=HFactory.createRangeSlicesQuery(keyspace, dt, ls,ds);
		//ColumnSliceIterator<Date, UUID, Double> csit=new ColumnSliceIterator<Date, UUID, Double>(sl,null,FINISH,false);
		sl.setColumnFamily("rhu2").setRange(null, null, false, 10)
        .setRowCount(row_count);
		Date Lastkey=null;
	
		
		/*QueryResult<ColumnSlice<UUID,Double>> qr=sl.execute();
		System.out.println("\nInserted data is as follows:\n" + qr.get());*/
		while (true) {
				sl.setKeys(Lastkey,null);
			 QueryResult<OrderedRows<Date, Long, Double>> result = sl.execute();
	            OrderedRows<Date, Long, Double> rows = result.get();
	            Iterator<Row<Date, Long, Double>> rowsIterator = rows.iterator();

	            // we'll skip this first one, since it is the same as the last one from previous time we executed
	            if (Lastkey != null && rowsIterator != null) rowsIterator.next();   

	            while (rowsIterator.hasNext()) {
	              Row<Date, Long, Double> row = rowsIterator.next();
	              Lastkey = row.getKey();

	              if (row.getColumnSlice().getColumns().isEmpty()) {
	                continue;
	              }


	              System.out.println(row);
	            }

	            if (rows.getCount() < row_count)
	                break;
	        }
			
		}
			
		}
		
	
	



