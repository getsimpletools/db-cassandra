<?php

namespace Simpletools\Db\Cassandra\Type;

class Inet implements \JsonSerializable
{
	protected $_value;

	public function __construct($content)
	{
		if ($content === null) $content = '';

		if (is_string($content))
			$this->_value = new \Cassandra\Inet($content);
		elseif ($content instanceof \Cassandra\Inet)
			$this->_value = $content;
		else
			throw new \Exception("Inet: Value is not string or inet object");
	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize() : mixed
	{
		return (int)$this->value();
	}

	public function __toString()
	{
		return (string)$this->value();
	}

	public function setDefault()
	{
		$this->_value = new \Cassandra\Inet('');
		return $this;
	}
}
