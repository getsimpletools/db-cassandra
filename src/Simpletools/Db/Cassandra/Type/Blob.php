<?php

namespace Simpletools\Db\Cassandra\Type;

class Blob implements \JsonSerializable
{
	protected $_value;

	public function __construct($content)
	{
		if($content === null) $content ='';

		if(is_string($content))
			$this->_value = new \Cassandra\Blob($content);
		elseif($content instanceof  \Cassandra\Blob)
			$this->_value = $content;
		else
			throw new \Exception("Blob: Value is not string or blob");
	}

	public function value()
	{
		return $this->_value;
	}

	public function jsonSerialize()
	{
		return (int) $this->value();
	}

	public function __toString()
	{
		return (string) $this->value();
	}

	public function setDefault()
	{
		$this->_value = new \Cassandra\Blob('');
		return $this;
	}

	public function getContent()
	{
		return $this->_value->toBinaryString();
	}
}
