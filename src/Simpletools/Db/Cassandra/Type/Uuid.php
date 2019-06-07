<?php

namespace Simpletools\Db\Cassandra\Type;

class Uuid implements \JsonSerializable
{
    protected $_value;

    public function __construct($uuid=null)
    {
			if($uuid !== null && !is_string($uuid) && $uuid instanceof \Cassandra\Uuid)
				throw new \Exception("Uuid: value is not a string or null");

			if($uuid instanceof \Cassandra\Uuid)
				$this->_value = $uuid;
			elseif($uuid)
					$this->_value = new \Cassandra\Uuid($uuid);
			else
					$this->_value = new \Cassandra\Uuid();
    }

    public function value()
    {
        return $this->_value;
    }

    public function jsonSerialize()
    {
        return (string)$this->value();
    }

    public function __toString()
    {
        return (string)$this->value();
    }
}
