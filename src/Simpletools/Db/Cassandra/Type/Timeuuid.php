<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Exception;

class Timeuuid implements \JsonSerializable
{
    protected $_value;

    public function __construct($timeSec=null)
    {
        if(is_numeric($timeSec)) {

            echo (int) $timeSec;
            $this->_value = new \Cassandra\Timeuuid((int)$timeSec);
        }
        elseif($timeSec!==null)
            throw new Exception("Wrong argument type",401);
        else
            $this->_value = new \Cassandra\Timeuuid();
    }

    public function value()
    {
        return ($this->_value = (string) $this->_value);
    }

    public function jsonSerialize()
    {
        return $this->value();
    }

    public function __toString()
    {
        return $this->value();
    }

    public function time()
    {
        return $this->_value->time();
    }
}
