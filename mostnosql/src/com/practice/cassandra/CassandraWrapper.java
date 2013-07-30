package com.practice.cassandra;

import java.util.Arrays;


import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraWrapper {
	public Cluster myCluster=null;
	public static String keyspaceName= "mostkeyspace";
	public static String cfdatapoint="datapoint";
	public String cfcreate=null;
	public KeyspaceDefinition ksdef=null;
	void setEnvironment()
	{
		myCluster = HFactory.getOrCreateCluster("Test Sample","localhost:9160");
		ksdef=myCluster.describeKeyspace(keyspaceName);
		if(ksdef==null)
		addKeyspacetoCassandra();		
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
			ColumnFamilyDefinition cfdef=HFactory.createColumnFamilyDefinition(keyspaceName, cfcreate);
	
		}
	}

}
