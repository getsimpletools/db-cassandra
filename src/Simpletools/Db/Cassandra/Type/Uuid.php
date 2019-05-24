<?php

namespace Simpletools\Db\Cassandra\Type;

class Uuid implements \JsonSerializable
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
        return (string)$this->_id;
    }

		public function jsonSerialize() {
			return $this->id();
		}

    public function __toString()
    {
        return $this->id();
    }
}
