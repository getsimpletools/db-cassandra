<?php

namespace Simpletools\Db\Cassandra;

class Async
{
	protected $_queries = array();
	protected $_client;
	protected $_runOnBatchSize = 10000; //0 - only manual batch run()
	protected $_requestTimeout = 5;

	public function __construct($batchSize = 10000, $requestTimeout = 5, $client = null)
	{
		if ($client !== null)
		{
			if(!($client instanceof Client))
			{
				throw new \Exception("Provided client is not an instance of \Simpletools\Db\Cassandra\Client", 404);
			}
			$this->_client = $client;
		}

		$this->_runOnBatchSize = $batchSize;
		$this->_requestTimeout = $requestTimeout;

		if(!$this->_client)
			$this->_client = new Client();

		$this->_client->connect();
	}


	public function add($query)
	{
		if($query instanceof Doc)
		{
			$query = $query->getSaveQuery();
		}

		if(!($query instanceof Query))
		{
			throw new Exception("Query is not of a Query type",400);
		}

		$query = $query->getQuery(true);

		$this->_queries[] = $this->_client->connector()->executeAsync($query['preparedQuery'],[
				'arguments' => $query['arguments']
		]);

		if($this->_runOnBatchSize && count($this->_queries) >= $this->_runOnBatchSize)
		{
			$this->run();
		}

		return $this;
	}


	public function run()
	{
		if(!$this->_queries)
		{
			throw new Exception("Empty async bulk",400);
		}

		foreach ($this->_queries as $future)
		{
			$future->get(5);
		}

		$this->reset();

		return true;
	}

	public function runIfNotEmpty()
	{
		if(!$this->_queries)
			return false;
		else
			return $this->run();
	}

	public function reset()
	{
		$this->_queries   = array();

		return $this;
	}

	public function __destruct()
	{
		$this->runIfNotEmpty();
	}
}
