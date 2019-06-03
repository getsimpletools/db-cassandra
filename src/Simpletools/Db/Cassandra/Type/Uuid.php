<?php

namespace Simpletools\Db\Cassandra\Type;

class Uuid implements \JsonSerializable
{
    protected $_value;

    public function __construct($uuid=null)
    {
        if($uuid)
            $this->_value = $uuid;
        else
            $this->_value = new \Cassandra\Uuid();
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
}
