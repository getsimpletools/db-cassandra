<?php

namespace Simpletools\Db\Cassandra;

class Schema
{
    protected $_tableName;
    protected $_definition;

    protected $_thisCreated = false;

    public function __construct($name)
    {
        $this->_tableName = $name;
    }

    public function describe($definition)
    {
        $this->_definition = json_decode(json_encode($definition));

        return $this;
    }

    public function create()
    {
        (new Client())->execute($this->toCql());

        $this->_thisCreated = true;
        return $this;
    }

    public function name($name=null)
    {
        if(!$name) return $this->_tableName;

        $this->_tableName = $name;

        return $this;
    }

    public function keyspace()
    {
        return $this->_definition->keyspace;
    }

    public function toCql()
    {
        $tableName = $this->_tableName;

        $settings = $this->_definition;

        if(!@$settings->keyspace)
            $settings->keyspace = (new Client())->keyspace();

        $columns = [];
        $clusteringOrder = [];
        foreach ($settings->columns as $column => $type)
            $columns[] = "\"" . $column . "\" " . strtolower($type);
        $primary = "PRIMARY KEY ((\"" . implode("\", \"", $settings->partition) . "\")";
        if (isset($settings->clustering))
        {
            $clusteringKeys = [];
            foreach ($settings->clustering as $key => $order)
            {
                $clusteringKeys[] = $key;
                $clusteringOrder[] = "\"" . $key . "\" " . strtoupper($order);
            }
            $columns[] = $primary . ", \"" . implode("\", \"", $clusteringKeys) . "\")";
        }
        else
        {
            $columns[] = $primary . ")";
        }

        $_query = "CREATE TABLE IF NOT EXISTS " . $settings->keyspace . ".\"" . $tableName . "\" (\n";
        $_query .= implode(", \n", $columns);
        $_query .= ")";

        if (count($clusteringOrder))
        {
            $_query .= " \nWITH CLUSTERING ORDER BY (" . implode(", ", $clusteringOrder) . ")";
        }

        $_parts = [];
        if(isset($settings->options) && is_iterable($settings->options))
        {
            foreach ($settings->options as $option => $value) {
                $_parts[] = "\"" . $option . "\" = " . $this->prepareOptionValue($value);
            }

            if (count($_parts)) {
                if (count($clusteringOrder)) {
                    $_query .= " \nAND";
                } else {
                    $_query .= " WITH";
                }
                $_query .= " " . implode(" \nAND ", $_parts);
            }
        }

        return $_query;
    }

    /* HELPER METHODS */
    public function prepareOptionValue($value)
    {
        if (is_object($value))
        {
            $value = json_encode($value);
            $value = str_replace('\"', "%SQUOTE%", $value);
            $value = str_replace("\"", "'", $value);
            $value = str_replace("%SQUOTE%", '"', $value);
        }
        else if (is_array($value))
        {
            $value = json_encode($value);
        }
        else if (is_string($value))
        {
            $value = "'" . $value . "'";
        }
        return $value;
    }

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
