<?php

namespace Simpletools\Db\Cassandra\Type;

class Date
{
    protected $_id;

    public function __construct($seconds)
    {
        $this->_id = (string) new \Cassandra\Date($seconds);
    }

    public function id()
    {
        return $this->_id;
    }

    public function __toString()
    {
        return $this->id();
    }
}