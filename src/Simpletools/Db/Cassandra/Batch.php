<?php

namespace Simpletools\Db\Cassandra;

use Simpletools\Db\Cassandra\Type\Uuid;

class Batch
{
    const LOGGED            = 0;
    const UNLOGGED          = 1;
    const BATCH_COUNTER     = 2;

    protected $_queries = array();
    protected $_client;

    protected $_hasRun  = false;
    protected $_queriesParsed = array();

    protected $_runOnBatchSize = 0;

    public function __construct($type = self::LOGGED)
    {
        $this->_type = $type;
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

    public function add($query)
    {
        if(!($query instanceof Query))
        {
            throw new Exception("Query is not of a Query type",400);
        }

        $this->_queries[] = $query;

        if(count($this->_queries)==$this->_runOnBatchSize)
        {
            $this->run();
        }

        return $this;
    }

    public function query($table)
    {
        $q = new Query($table);

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
        $_queries[] = "BEGIN BATCH";

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

    public function run()
    {
        if($this->_hasRun)
        {
            throw new Exception("This batch has run, reset() and try again",400);
        }

        if(!$this->_queries)
        {
            throw new Exception("Empty batch",400);
        }

        if(!$this->_queriesParsed) {

            foreach ($this->_queries as $query) {
                $q = $query->getQuery(true);
                $this->_queriesParsed[] = ['statement' => $q['preparedQuery'], 'args' => $q['arguments']];
            }
        }

        $batch = new \Cassandra\BatchStatement($this->_type);

        if(!$this->_client)
            $this->_client = new Client();

        $this->_client->connect();

        foreach($this->_queriesParsed as $q) {
            $batch->add($q['statement'], $q['args']);
        }

        $res = $this->_client->connector()->execute($batch);
        unset($batch);

        $this->_hasRun = true;

        $this->reset();

        if($res) return true;
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

        return $this;
    }

    public function runEvery($batchSize)
    {
        $this->_runOnBatchSize = (int) $batchSize;

        return $this;
    }
}
