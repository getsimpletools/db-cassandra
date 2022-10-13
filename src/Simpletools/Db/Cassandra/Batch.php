<?php

namespace Simpletools\Db\Cassandra;
use Simpletools\Db\Replicator;

class Batch
{
    const LOGGED            = 0;
    const UNLOGGED          = 1;
    const BATCH_COUNTER     = 2;

    protected $_queries = array();
    protected $_client;

    protected $_hasRun  = false;
    protected $_queriesParsed = array();
    protected $_table = null;
    protected $_keyspace = null;
    protected $_replication = false;
    protected $_replicationQuery;
    protected $___consistency = null;

    protected $_runOnBatchSize = 0; //0 - only manual batch run()
		protected $_bubbles=[];
		protected $_bubble;

    public function __construct($type = self::LOGGED)
    {
        $this->_type = $type;
        $this->_replicationQuery =  (object)[
					'insert' => [],
					'update' =>[],
					'delete' =>[]
				];
    }

    public function client($client)
    {
        if (!($client instanceof Client))
        {
            throw new \Exception("Provided client is not an instance of \Simpletools\Db\Cassandra\Client", 404);
        }

        $this->_client = $client;

        return $this;
    }

    public function constraint($table, $keyspace = null)
		{
			$this->_table = $table;
			$this->_keyspace = $keyspace;
			return $this;
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

        $this->_queries[] = $query;

        if($this->_runOnBatchSize && count($this->_queries)==$this->_runOnBatchSize)
        {
            $this->run();
        }

        return $this;
    }

    public function query($table,  $keyspace = null)
    {
        $q = new Query($table, $keyspace);

        $this->add($q);

        return $q;
    }

    public function getQuery($delimeter="\n")
    {
        if(!$this->_queries)
        {
            throw new Exception("Empty batch",400);
        }

        $_queries = array();
        $_queries[] = "BEGIN UNLOGGED BATCH";

        foreach($this->_queries as $query)
        {
            $_queries[] = trim(($query->getQuery())).';';
        }

        $_queries[] = "APPLY BATCH;";

        return implode($delimeter,$_queries);
    }

    public function size()
    {
        return count($this->_queries);
    }

    public function getBatch()
		{
			if($this->_hasRun)
			{
				throw new Exception("This batch has run, reset() and try again",400);
			}

			if(!$this->_queries)
			{
				throw new Exception("Empty batch",400);
			}

			if(!$this->_client)
				$this->_client = new Client();

			if($this->_table)//constraint
			{
				if($this->_keyspace === null)
					$this->_keyspace = $this->_client->keyspace();

				$this->_replication = Replicator::exists('cassandra://bulk@'.$this->_keyspace.'.'.$this->_table);

				if(!$this->_queriesParsed)
				{
					foreach ($this->_queries as $query)
					{
						$rawQuery = $query->getRawQuery();

						if($rawQuery['db'] != $this->_keyspace || $rawQuery['table'] != $this->_table)
							throw new \Exception("Your batch Query(".$rawQuery['db'].'.'.$rawQuery['table'].") does not match constraint(".$this->_keyspace.'.'.$this->_table.")");


						if($this->_replication)
						{
							if(@$rawQuery['type'] == 'INSERT')
							{
								$this->_replicationQuery->insert[] = $query->getRawQueryData($rawQuery);
							}
							elseif (@$rawQuery['type'] == 'UPDATE')
							{
								if($data = $query->getRawQueryData($rawQuery))
								{
									$this->_replicationQuery->update[] = $data;
								}
							}
							elseif (@$rawQuery['type'] == 'DELETE FROM')
							{
								if($data = $query->getRawQueryData($rawQuery))
								{
									$this->_replicationQuery->delete[] = $data;
								}
							}
						}

						$q = $query->getQuery(true);
						$this->_queriesParsed[] = array('statement' => $q['preparedQuery'], 'args' => $q['arguments']);
					}
				}
			}
			elseif($this->_bubble)
			{
				if(!$this->_queriesParsed)
				{
					foreach ($this->_queries as $query)
					{
						$rawQuery = $query->getRawQuery();

						if(@$rawQuery['type'] == 'INSERT')
						{
							$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['bulk']['insert'][] = $query->getRawQueryData($rawQuery);
						}
						elseif (@$rawQuery['type'] == 'UPDATE')
						{
							if($data = $query->getRawQueryData($rawQuery))
								$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['bulk']['update'][] = $data;
						}
						elseif (@$rawQuery['type'] == 'DELETE FROM')
						{
							if($data = $query->getRawQueryData($rawQuery))
								$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['bulk']['delete'][] = $data;

						}

						$q = $query->getQuery(true);
						$this->_queriesParsed[] = array('statement' => $q['preparedQuery'], 'args' => $q['arguments']);
					}
				}
			}
			else
			{
				if(!$this->_queriesParsed)
				{
					foreach ($this->_queries as $query)
					{
						if($query->isBubble())
						{
							$rawQuery = $query->getRawQuery();
							if(@$rawQuery['type'] == 'INSERT')
							{
								$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['write'][] = $query->getRawQueryData($rawQuery);
							}
							elseif (@$rawQuery['type'] == 'UPDATE')
							{
								if($data = $query->getRawQueryData($rawQuery))
									$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['update'][] = $data;
							}
							elseif (@$rawQuery['type'] == 'DELETE FROM')
							{
								if($data = $query->getRawQueryData($rawQuery))
									$this->_bubbles[$rawQuery['db'].'.'.$rawQuery['table']]['delete'][] = $data;
							}
						}

						$q = $query->getQuery(true);
						$this->_queriesParsed[] = array('statement' => $q['preparedQuery'], 'args' => $q['arguments']);
					}
				}
			}

			$batch = new \Cassandra\BatchStatement($this->_type);

			foreach($this->_queriesParsed as $q) {
				$batch->add($q['statement'], $q['args']);
			}

			return $batch;
		}

    public function consistency($consistency=null)
    {
        if($consistency===null) return $this->___consistency;

        $this->___consistency = $consistency;

        return $this;
    }

    protected $___options = array();
    public function options($options=array())
    {
        $this->___options = $options;

        return $this;
    }

    public function run()
    {
        if(!$this->_client)
            $this->_client = new Client();

        $this->_client->connect();

        $options = $this->___options;

        if($this->___consistency!==null) {
            $options = array_merge($options, [
                'consistency' => $this->___consistency
            ]);
        }

        $res = $this->_client->executeWithReconnect($this->_client->connector(),$this->getBatch(),$options);
        //unset($batch);

        $this->replicate();;
        $this->runBubbles();
        $this->_hasRun = true;

        $this->reset();

        if($res) return true;
    }

    public function replicate()
		{
			if($this->_replication)
			{
				Replicator::trigger('cassandra://bulk@'.$this->_keyspace.'.'.$this->_table, $this->_replicationQuery);
			}
		}

		public function runBubbles()
		{
			if($this->_bubbles)
			{
				foreach ($this->_bubbles as  $keyspaceTable => $actions)
				{
					foreach ($actions as $action => $items)
					{
						if($action == 'bulk')
							Replicator::trigger('cassandra://'.$action.'@'.$keyspaceTable, (object) $items);
						else
						{
							foreach ($items as $item)
							{
								Replicator::trigger('cassandra://'.$action.'@'.$keyspaceTable, (object) $item);
							}
						}
					}
				}
			}
		}


		public function isReplication()
		{
			return $this->_replication;
		}

		public function getReplicationQuery()
		{
			return (object)[
				'keyspace' => $this->_keyspace,
				'table' => $this->_table,
				'data' => $this->_replicationQuery
			];
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
        $this->_hasRun          = false;
        $this->_queries         = array();
        $this->_queriesParsed   = array();
				$this->_replicationQuery = (object)[
					'insert' => [],
					'update' =>[],
					'delete' =>[]
				];
				$this->_bubbles =[];

        return $this;
    }

    public function runEvery($batchSize)
    {
        $this->_runOnBatchSize = (int) $batchSize;

        return $this;
    }

		public function bubble()
		{
			$this->_bubble = true;
        return $this;
    }
}
