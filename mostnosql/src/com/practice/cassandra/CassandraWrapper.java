package com.practice.cassandra;

import java.util.Arrays;


import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraWrapper {
	public static Cluster myCluster = HFactory.getOrCreateCluster("Test Sample","localhost:9160");
	void addKeyspacetoCassandra()
	{
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("mostkeyspace", "datapoint");
		KeyspaceDefinition kfdef=HFactory.createKeyspaceDefinition("mostkeyspace",  "org.apache.cassandra.locator.SimpleStrategy",1, Arrays.asList(cfDef));
		myCluster.addKeyspace(kfdef);
		
	}

}
