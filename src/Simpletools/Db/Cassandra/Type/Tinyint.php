<?php

namespace Simpletools\Db\Cassandra\Type;

class Tinyint implements \JsonSerializable
{
	protected $_value;

	public function __construct($int)
	{
		if(is_numeric($int))
			$this->_value = new \Cassandra\Tinyint((int)$int);
		else
			throw new \Exception("BigInt: Value is not numeric");

	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize() : mixed
	{
		return (int) $this->value();
	}

	public function __toString()
	{
		return (string) $this->value();
	}

	public function toInt()
	{
		return (int) $this->value();
	}

	public function setDefault()
	{
		$this->_value = new \Cassandra\Tinyint(0);
		return $this;
	}
}
