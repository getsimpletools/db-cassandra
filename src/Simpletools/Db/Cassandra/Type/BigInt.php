<?php

namespace Simpletools\Db\Cassandra\Type;

class BigInt
{
    protected $_id;

    public function __construct($bigint)
    {
        $this->_id = new \Cassandra\BigInt((string) $bigint);
    }

    public function id()
    {
        return $this->_id;
    }

    public function value()
    {
        return (string) $this->_id;
    }

    public function __toString()
    {
        return (string) $this->id();
    }

    public function toInt()
    {
        return (int) (string) $this->id();
    }
}