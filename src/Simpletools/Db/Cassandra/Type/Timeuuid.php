<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Exception;

class Timeuuid implements \JsonSerializable
{
    protected $_id;

    public function __construct($timeSec=null)
    {
        if(is_numeric($timeSec)) {

            echo (int) $timeSec;
            $this->_id = new \Cassandra\Timeuuid((int)$timeSec);
        }
        elseif($timeSec!==null)
            throw new Exception("Wrong argument type",401);
        else
            $this->_id = new \Cassandra\Timeuuid();
    }

    public function id()
    {
        return ($this->_id = (string) $this->_id);
    }

    public function jsonSerialize()
    {
        return $this->id();
    }

    public function __toString()
    {
        return $this->id();
    }

    public function time()
    {
        return $this->_id->time();
    }
}
