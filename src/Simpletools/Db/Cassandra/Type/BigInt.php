<?php

namespace Simpletools\Db\Cassandra\Type;

class BigInt
{
    protected $_value;

    public function __construct($bigint)
    {
        $this->_value = new \Cassandra\BigInt((string) $bigint);
    }

    public function value()
    {
        return $this->_value;
    }

//    public function value()
//    {
//        return (string) $this->_value;
//    }

    public function __toString()
    {
        return (string) $this->value();
    }

    public function toInt()
    {
        return (int) (string) $this->value();
    }
}
