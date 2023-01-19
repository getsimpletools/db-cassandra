<?php

namespace Simpletools\Db\Cassandra\Type;

class Decimal implements \JsonSerializable
{
	protected $_value;

	public function __construct($int)
	{
		if(is_numeric($int))
			$this->_value = new \Cassandra\Decimal((float)$int);
		else
			throw new \Exception("Decimal: Value is not numeric");
	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize() : mixed
	{
		return (float) $this->value();
	}

	public function __toString()
	{
		return (string) $this->value();
	}

	public function toFloat()
	{
		return (float) $this->value();
	}

	public function setDefault()
	{
		$this->_value = new \Cassandra\Decimal((float)0);
		return $this;
	}
}
