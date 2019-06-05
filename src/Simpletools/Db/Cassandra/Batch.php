<?php

namespace Simpletools\Db\Cassandra;

use Simpletools\Db\Cassandra\Type\Uuid;

class Batch extends Client
{
    const LOGGED            = 0;
    const UNLOGGED          = 1;
    const BATCH_COUNTER     = 2;

    protected $_queries = array();

    public function __construct($type = self::LOGGED)
    {
        $this->_type = $type;
        $this->_batch = new \Cassandra\BatchStatement($this->_type);

        parent::__construct();
    }

    public function add($query)
    {
        if(!($query instanceof Query))
        {
            throw new Exception("Query is not of a Query type",400);
        }

        $q = ($query->getQuery(true));

        foreach($q['arguments'] as $arg)
        {
            if($arg instanceof Uuid)
                $args[] = new \Cassandra\Uuid($arg->value());
            elseif(is_object($arg))
                $args[] = $arg->value();
            else
                $args[] = $arg;
        }

        $this->_queries[] = ['statement'=>$q['preparedQuery'],'args'=>$args];

        return $this;
    }

    public function query($table)
    {
        $q = new Query($table);

        $this->add($q);

        return $q;
    }

    public function run()
    {
        if(!$this->_queries)
        {
            throw new Exception("Empty batch",400);
        }

        $this->connect();

        foreach($this->_queries as $q) {
            $this->_batch->add($q['statement'], $q['args']);
        }

        $this->_queries = [];
        $res = $this->___connection->execute($this->_batch);

        unset($this->_batch);
        $this->_batch = new \Cassandra\BatchStatement($this->_type);

        if($res) return true;
    }
}