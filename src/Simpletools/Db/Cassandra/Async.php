<?php

namespace Simpletools\Db\Cassandra;
use Simpletools\Db\Replicator;

class Async
{
	protected $_queries = array();
	protected $_client;
	protected $_runOnBatchSize = 10000; //0 - only manual batch run()
	protected $_requestTimeout = 5;
	protected $_replicationQuery = [];
	protected $_replication = true;

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

	public function disableReplication()
	{
		$this->_replication = false;
	}

	public function add($query)
	{
		if($query instanceof  Batch)
		{
			if(!$query->size())
				return $this;

			$batchId = uniqid();
			$this->_queries[$batchId] = $this->_client->connector()->executeAsync($query->getBatch());

			if($query->isReplication())
			{

				$repQuery = $query->getReplicationQuery();
				$this->_replicationQuery[$batchId] = (object)[
					'data' => $repQuery->data,
					'type' => 'bulk',
					'dest' => $repQuery->keyspace.'.'.$repQuery->table
				];
			}

			$query->reset();

			if($this->_runOnBatchSize && count($this->_queries) >= $this->_runOnBatchSize)
			{
				$this->run();
			}

			return $this;
		}

		if($query instanceof Doc)
		{
			$query = $query->getSaveQuery();
		}

		if(!($query instanceof Query))
		{
			throw new Exception("Query is not of a Query type",400);
		}


		if($this->_replication)
		{
			$rawQuery = $query->getRawQuery();
			$callId = uniqid();

			$type = false;
			if($rawQuery['type'] == 'INSERT') $type ='write';
			elseif($rawQuery['type'] == 'UPDATE') $type ='update';
			elseif($rawQuery['type'] == 'DELETE FROM') $type ='delete';

			if($type && Replicator::exists('cassandra://'.$type.'@'.$rawQuery['db'].'.'.$rawQuery['table']))
			{
				if($data = $query->getRawQueryData($rawQuery))
				{
					$this->_replication = true;
					$this->_replicationQuery[$callId] = (object)[
						'type' =>  $type,
						'data' => $data,
						'dest' => $rawQuery['db'].'.'.$rawQuery['table']
					];
				}
			}

			$query = $query->getQuery(true);

			$this->_queries[$callId] = $this->_client->connector()->executeAsync($query['preparedQuery'],[
				'arguments' => $query['arguments']
			]);
		}
		else
		{
			$query = $query->getQuery(true);

			$this->_queries[] = $this->_client->connector()->executeAsync($query['preparedQuery'],[
				'arguments' => $query['arguments']
			]);
		}

		if($this->_runOnBatchSize && count($this->_queries) >= $this->_runOnBatchSize)
		{
			$this->run();
		}

		return $this;
	}

	public function run($timeout = 5)
	{
		if(!$this->_queries)
		{
			throw new Exception("Empty async bulk",400);
		}

		if($this->_replication)
		{
			foreach ($this->_queries as $queryId => $future)
			{
				$future->get($this->_requestTimeout);
				unset($this->_queries[$queryId]);

				if(isset($this->_replicationQuery[$queryId]))
				{
					Replicator::trigger('cassandra://'.$this->_replicationQuery[$queryId]->type.'@'.$this->_replicationQuery[$queryId]->dest, $this->_replicationQuery[$queryId]->data);
					unset($this->_replicationQuery[$queryId]);
				}
			}
		}
		else
		{
			foreach ($this->_queries as $queryId => $future)
			{
				$future->get($this->_requestTimeout);
				unset($this->_queries[$queryId]);
			}
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
		$this->_replicationQuery =  [];

		return $this;
	}

	public function __destruct()
	{
		$this->runIfNotEmpty();
	}
}
