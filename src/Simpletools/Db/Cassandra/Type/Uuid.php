<?php

namespace Simpletools\Db\Cassandra\Type;

class Uuid
{
    protected $_id;

    public function __construct($uuid=null)
    {
        if($uuid)
            $this->_id = (string) $uuid;
        else
            $this->_id = (string) new \Cassandra\Uuid();
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