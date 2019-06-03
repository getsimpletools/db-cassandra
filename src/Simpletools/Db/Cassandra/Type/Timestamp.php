<?php

namespace Simpletools\Db\Cassandra\Type;

class Timestamp implements \JsonSerializable
{
    protected $_value;

    public function __construct($time=null)
    {
        //$this->_microtime = microtime(true);
        //$this->_id = (int) floor($this->_microtime*1000);

        if(is_string($time))
        {
            $time = strtotime($time);
        }

        if($time)
            $this->_value = new \Cassandra\Timestamp($time);
        else
            $this->_value = new \Cassandra\Timestamp();
    }

    public function value()
    {
        return $this->_value;
    }

    public function jsonSerialize()
    {
        return $this->value();
    }

    public function __toString()
    {
        return (string) $this->value();
    }

    public function time()
    {
        return $this->_value->time();
    }

    public function microtime($get_as_float=null)
    {
        return $this->_value->microtime($get_as_float);
    }

    public function toDateTime()
    {
        return $this->_value->toDateTime();
    }
}

