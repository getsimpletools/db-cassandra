<?php

namespace Simpletools\Db\Cassandra\Type;

class Timestamp implements \JsonSerializable
{
    protected $_id;

    public function __construct($time=null)
    {
        //$this->_microtime = microtime(true);
        //$this->_id = (int) floor($this->_microtime*1000);

        if(is_string($time))
        {
            $time = strtotime($time);
        }

        if($time)
            $this->_id = new \Cassandra\Timestamp($time);
        else
            $this->_id = new \Cassandra\Timestamp();
    }

    public function id()
    {
        return $this->_id;
    }

    public function jsonSerialize()
    {
        return $this->id();
    }

    public function __toString()
    {
        return (string) $this->id();
    }

    public function time()
    {
        return $this->_id->time();
    }

    public function microtime($get_as_float=null)
    {
        return $this->_id->microtime($get_as_float);
    }

    public function toDateTime()
    {
        return $this->_id->toDateTime();
    }
}

