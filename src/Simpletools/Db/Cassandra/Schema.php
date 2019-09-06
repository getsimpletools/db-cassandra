<?php

namespace Simpletools\Db\Cassandra;

class Schema
{
	protected static $_schema = array();

	public static function getSchema(Client $client, $keyspace, $table)
	{
		$cluster = $client->getCluster();

		if(isset(self::$_schema[$cluster][$keyspace][$table]['schema']))
			return self::$_schema[$cluster][$keyspace][$table]['schema'];

		self::$_schema[$cluster][$keyspace][$table]['schema'] = array();
		self::$_schema[$cluster][$keyspace][$table]['primaryKey'] = array();
		self::$_schema[$cluster][$keyspace][$table]['clusteringKey'] = array();
		self::$_schema[$cluster][$keyspace][$table]['partitionKey'] = array();

		$schema   = $client->connector()->schema();
		$keyspaceObj = $schema->keyspace($keyspace);

		if($keyspaceObj===false)
		{
			throw new Exception("Provided keyspace (".$keyspace.") doesn't exist",404);
		}

		$tableObj   = $keyspaceObj->table($table);

		if(!$tableObj)
		{
			$tableObj   = $keyspaceObj->materializedView($table);
			if(!$tableObj)
			{
				throw new Exception("Provided table (".$table.") doesn't exist",404);
			}
		}

		foreach ($tableObj->columns() as $column)
		{
			self::$_schema[$cluster][$keyspace][$table]['schema'][$column->name()] = (string)$column->type();
		}


		foreach ($tableObj->partitionKey() as $column)
		{
			self::$_schema[$cluster][$keyspace][$table]['partitionKey'][] = $column->name();
		}

		foreach ($tableObj->primaryKey() as $column)
		{
			self::$_schema[$cluster][$keyspace][$table]['primaryKey'][] = $column->name();
		}

		foreach ($tableObj->clusteringKey() as $column)
		{
			self::$_schema[$cluster][$keyspace][$table]['clusteringKey'][] = $column->name();
		}

		return self::$_schema[$cluster][$keyspace][$table]['schema'];
	}

	public static function getPrimaryKey(Client $client, $keyspace, $table)
	{
		$cluster = $client->getCluster();

		if(isset(self::$_schema[$cluster][$keyspace][$table]['primaryKey']))
			return self::$_schema[$cluster][$keyspace][$table]['primaryKey'];

		self::getSchema($client, $keyspace, $table);

		return self::$_schema[$cluster][$keyspace][$table]['primaryKey'];
	}

	public static function getPartitionKey(Client $client, $keyspace, $table)
	{
		$cluster = $client->getCluster();

		if(isset(self::$_schema[$cluster][$keyspace][$table]['partitionKey']))
			return self::$_schema[$cluster][$keyspace][$table]['partitionKey'];

		self::getSchema($client, $keyspace, $table);

		return self::$_schema[$cluster][$keyspace][$table]['partitionKey'];
	}

	public static function getClusteringKey(Client $client, $keyspace, $table)
	{
		$cluster = $client->getCluster();

		if(isset(self::$_schema[$cluster][$keyspace][$table]['clusteringKey']))
			return self::$_schema[$cluster][$keyspace][$table]['clusteringKey'];

		self::getSchema($client, $keyspace, $table);

		return self::$_schema[$cluster][$keyspace][$table]['clusteringKey'];
	}


}
