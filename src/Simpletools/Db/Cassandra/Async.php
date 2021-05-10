<?php

namespace Simpletools\Db\Cassandra;
use Simpletools\Db\Cassandra\Type\Uuid;
use Simpletools\Db\Replicator;

class Async
{
	protected $_queries = array();
	protected $_queriesCache = array(); //[$query, $options, $retried]
	protected $_client;
	protected $_runOnBatchSize = 10000; //0 - only manual batch run()
	protected $_requestTimeout = 5;
	protected $_replicationQuery = [];
	protected $_replication = true;
	protected $_retryPolicy = 'reconnect'; //(reconnect | fallthrough | silence )

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

	/**
	 * @param string $policy (reconnect | fallthrough | silence )
	 */
	public function setRetryPolicy(string $policy)
	{
		if(!in_array($policy,['reconnect', 'fallthrough', 'silence']))
			throw  new Exception('Unknow retryPolicy: '.$policy.' Use: (reconnect | fallthrough | silence)');

		$this->_retryPolicy = $policy;
		return $this;
	}

	public function disableReplication()
	{
		$this->_replication = false;
		return $this;
	}

	public function add($query)
	{
		$streamId = (string)(new Uuid());
		if($query instanceof Batch)
		{
			if(!$query->size())
				return $this;

			$options = array();

            $consistency = $query->consistency();
			if($consistency!==null)
            {
                $options['consistency'] = $consistency;
            }

			$rawQuery = $query->getBatch();
			$this->_queries[$streamId] = $this->_client->connector()->executeAsync($rawQuery,$options);
			if($this->_retryPolicy =='reconnect')
				$this->_queriesCache[$streamId] = [$rawQuery,$options, 0];

			if($query->isReplication())
			{
				$repQuery = $query->getReplicationQuery();
				$this->_replicationQuery[$streamId] = (object)[
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

        $consistency = $query->consistency();

		if($this->_replication)
		{
			$rawQuery = $query->getRawQuery();

			$type = false;
			if($rawQuery['type'] == 'INSERT') $type ='write';
			elseif($rawQuery['type'] == 'UPDATE') $type ='update';
			elseif($rawQuery['type'] == 'DELETE FROM') $type ='delete';

			if($type && Replicator::exists('cassandra://'.$type.'@'.$rawQuery['db'].'.'.$rawQuery['table']))
			{
				if($data = $query->getRawQueryData($rawQuery))
				{
					$this->_replication = true;
					$this->_replicationQuery[$streamId] = (object)[
						'type' =>  $type,
						'data' => $data,
						'dest' => $rawQuery['db'].'.'.$rawQuery['table']
					];
				}
			}

			$query = $query->getQuery(true);

			$options = ['arguments' => $query['arguments']];
			if($consistency!==null)
                $options['consistency'] = $consistency;

			$this->_queries[$streamId] = $this->_client->connector()->executeAsync($query['preparedQuery'],$options);
			if($this->_retryPolicy =='reconnect')
			{
				$this->_queriesCache[$streamId] = [$query['preparedQuery'],$options, 0];
			}
		}
		else
		{
			$query = $query->getQuery(true);

            $options = ['arguments' => $query['arguments']];
            if($consistency!==null)
                $options['consistency'] = $consistency;

			$this->_queries[$streamId] = $this->_client->connector()->executeAsync($query['preparedQuery'],$options);
			if($this->_retryPolicy =='reconnect')
			{
				$this->_queriesCache[$streamId] = [$query['preparedQuery'],$options,0];
			}
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


		if($this->_replication && $this->_retryPolicy=='reconnect')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				try{
					$future->get($this->_requestTimeout);
					unset($this->_queries[$streamId]);
					unset($this->_queriesCache[$streamId]);
				}catch (\Exception $e)
				{
					if (($e->getCode() == 0 ||$e->getCode() == 16777230 || $e->getCode() == '16777225'|| $e->getCode() == '33558784' || $e->getCode() == '33559040' ||  $e->getCode() == '33558529')
							&& isset($this->_queriesCache[$streamId]) && $this->_queriesCache[$streamId][2] < 3)
					{
						$this->_queriesCache[$streamId][2]++;
						$this->_queries[$streamId] = $this->_client->connector()->executeAsync($this->_queriesCache[$streamId][0],$this->_queriesCache[$streamId][1]);
					}
					else
						throw $e;
				}
			}
		}
		elseif($this->_replication && $this->_retryPolicy=='fallthrough')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				$future->get($this->_requestTimeout);
				unset($this->_queries[$streamId]);

				if(isset($this->_replicationQuery[$streamId]))
				{
					Replicator::trigger('cassandra://'.$this->_replicationQuery[$streamId]->type.'@'.$this->_replicationQuery[$streamId]->dest, $this->_replicationQuery[$streamId]->data);
					unset($this->_replicationQuery[$streamId]);
				}
			}
		}
		elseif($this->_replication && $this->_retryPolicy=='silence')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				try{
					$future->get($this->_requestTimeout);
					if(isset($this->_replicationQuery[$streamId]))
					{
						Replicator::trigger('cassandra://'.$this->_replicationQuery[$streamId]->type.'@'.$this->_replicationQuery[$streamId]->dest, $this->_replicationQuery[$streamId]->data);
						unset($this->_replicationQuery[$streamId]);
					}
				} catch (\Exception $e){}
				unset($this->_queries[$streamId]);
			}
		}
		elseif($this->_retryPolicy=='reconnect')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				try{
				$future->get($this->_requestTimeout);
					unset($this->_queries[$streamId]);
					unset($this->_queriesCache[$streamId]);
				}catch (\Exception $e)
				{
					if (($e->getCode() == 0 || $e->getCode() == 16777230 || $e->getCode() == '16777225' || $e->getCode() == '33558784' || $e->getCode() == '33559040' ||  $e->getCode() == '33558529') && isset($this->_queriesCache[$streamId]) && $this->_queriesCache[$streamId][2] < 3)
					{
						$this->_queriesCache[$streamId][2]++;
						$this->_queries[$streamId] = $this->_client->connector()->executeAsync($this->_queriesCache[$streamId][0],$this->_queriesCache[$streamId][1]);
					}
					else
							throw $e;
				}
			}
		}
		elseif($this->_retryPolicy=='fallthrough')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				$future->get($this->_requestTimeout);
				unset($this->_queries[$streamId]);
			}
		}
		elseif($this->_retryPolicy=='silence')
		{
			foreach ($this->_queries as $streamId => $future)
			{
				try{
					$future->get($this->_requestTimeout);
				} catch (\Exception $e){}
				unset($this->_queries[$streamId]);
			}
		}

		if($this->_queries) $this->run();

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
		$this->_queriesCache = [];

		return $this;
	}

	public function __destruct()
	{
		$this->runIfNotEmpty();
	}
}
