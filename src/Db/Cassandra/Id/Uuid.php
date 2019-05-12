<?php

namespace Simpletools\Db\Cassandra\Uuid;

class Uuid
{
    protected $_id;

    public function __construct($uuid=null)
    {
        if($uuid) $this->_id = $uuid;

        $this->_id = new \Cassandra\Uuid();
    }

    public function id()
    {
        return $this->_id = (string) $this->_id;
    }

    public function __toString()
    {
        return $this->id();
    }
}