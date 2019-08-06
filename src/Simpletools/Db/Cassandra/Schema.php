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

		foreach ($tableObj->primaryKey() as $column)
		{
			self::$_schema[$cluster][$keyspace][$table]['primaryKey'][] = $column->name();
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



}
