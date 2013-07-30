package com.practice.cassandra;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraWrapper {
	public Cluster myCluster=null;
	public static String keyspaceName= "mostkeyspace";
	public static String cfdatapoint="datapoint";
	public String cfcreate=null;
	public KeyspaceDefinition ksdef=null;
	public ColumnFamilyDefinition cfdef=null;
	public static Keyspace keyspace=null;
	void setEnvironment()
	{
		myCluster = HFactory.getOrCreateCluster("Test Sample","localhost:9160");
		ksdef=myCluster.describeKeyspace(keyspaceName);
		if(ksdef==null)
		addKeyspacetoCassandra();		
		keyspace=HFactory.createKeyspace("mostkeyspace", myCluster);
	}
	void addKeyspacetoCassandra()
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,cfdatapoint,ComparatorType.BYTESTYPE);
		int replicationFactor=1;
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,ThriftKsDef.DEF_STRATEGY_CLASS,replicationFactor,Arrays.asList(cfDef));
		//Add the schema to the cluster.
		myCluster.addKeyspace(newKeyspace);		
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
			if(!checkExist(cfcreate))
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
		cfdef=getCfdf(dpname);
		long timeInMicroSeconds=u.getTime();
		UUID timeUUIDColumnName = TimeUUIDUtils.getTimeUUID(timeInMicroSeconds);
		System.out.println(d+"\t"+timeUUIDColumnName+"\t"+value);
		DateSerializer ds=new DateSerializer();
		Mutator<Date> mu=HFactory.createMutator(keyspace, ds);
		mu.insert(d, cfdef.getName(), HFactory.createColumn(timeUUIDColumnName, value));
		
	}
}


