<?php

namespace Simpletools\Db\Cassandra\Type;

class Time implements \JsonSerializable
{
	protected $_value;

	public function __construct(string|int|float|null $time = null)
	{
		if(is_numeric($time) && strlen($time) <= 10)
			$this->_value = new \Cassandra\Time((int)$time);
		elseif (is_string($time) && ($time = strtotime($time)))
			$this->_value = new \Cassandra\Time($time);
		elseif ($time===null)
			$this->_value = new \Cassandra\Time();
		elseif ($time instanceof \Cassandra\Time)
			$this->_value = $time;
		else
			throw new \Exception("Timestamp: Value is not a timestamp or date");
	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize() : mixed
	{
		return $this->seconds();}

	public function __toString()
	{
		return (string) $this->value();
	}

	public function seconds()
	{
        return $this->_value->seconds();
	}

	public function setDefault()
	{
        $this->_value = new \Cassandra\Time();
        return $this;
	}

	public static function fromDateTime($dateTime)
    {
        return new static(\Cassandra\Time::fromDateTime($dateTime));
    }
}

