<?php

namespace Simpletools\Db\Cassandra\Type;

class Timestamp implements \JsonSerializable
{
	protected $_value;

	public function __construct($time = null)
	{
		if(is_numeric($time) && strlen($time) <= 10)
			$this->_value = new \Cassandra\Timestamp((int)$time);
		elseif (is_string($time) && ($time = strtotime($time)))
			$this->_value = new \Cassandra\Timestamp($time);
		elseif ($time===null)
			$this->_value = new \Cassandra\Timestamp();
		elseif ($time instanceof \Cassandra\Timestamp)
			$this->_value = $time;
		else
			throw new \Exception("Timestamp: Value is not a timestamp or date");

	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize()
	{
		return $this->toInt();
		//return (string)$this->value()->toDateTime()->format(DATE_ATOM);
	}

	public function __toString()
	{
		return substr((string)$this->value(),0,10);
	}

	public function toInt()
	{
		return (int)substr((string)$this->value(),0,10);
	}

	public function microtime($get_as_float=null)
	{
			return $this->_value->microtime($get_as_float);
	}

	public function toDateTime()
	{
			return $this->_value->toDateTime();
	}

	public function setDefault()
	{
			$this->_value = new \Cassandra\Timestamp(0);
			return $this;
	}
}

