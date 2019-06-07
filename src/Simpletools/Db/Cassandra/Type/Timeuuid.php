<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Exception;

class Timeuuid implements \JsonSerializable
{
    protected $_value;

    public function __construct($time=null)
    {
			if(is_numeric($time) && strlen($time) <= 10)
				$this->_value = new \Cassandra\Timeuuid((int)$time);
			elseif(is_string($time) && strlen($time) == 36)
				$this->_value = new \Cassandra\Timeuuid($time);
			elseif (is_string($time) && ($time = strtotime($time)))
				$this->_value = new \Cassandra\Timeuuid($time);
			elseif ($time===null)
				$this->_value = new \Cassandra\Timeuuid();
			elseif ($time instanceof \Cassandra\Timeuuid)
				$this->_value = $time;
			else
				throw new \Exception("Timeuuid: Value is not a timestamp or date");



    }

    public function value()
    {
        return $this->_value;
    }

		public function toInt()
		{
			return (int) $this->value();
		}

    public function jsonSerialize()
    {
        return $this->value();
    }

    public function __toString()
    {
        return (string)$this->value();
    }

	public function setDefault()
	{
		$this->_value = new \Cassandra\Timeuuid(0);
		return $this;
	}

    public function time()
    {
        return $this->_value->time();
    }
}
