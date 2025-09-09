<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Client;

class Set implements \JsonSerializable
{
	protected $_body;
	protected $_value;
	protected $_valueType;

	public function __construct($body, $valueType = \Cassandra::TYPE_TEXT)
	{
		if ($body instanceof \Cassandra\Set)
		{
			$this->_body = $body->values();
			$this->_valueType = $body->type()->valueType();
		}
		elseif(is_array($body))
		{
			$this->_body = $body;
			$this->_valueType = $valueType;
		}
		else
			throw new \Exception("Set: Type must be Cassandra\Set or Array");
	}


	public function value()
	{
		$this->_value = new \Cassandra\Set($this->_valueType);

    if($this->_body)
    {
      foreach ($this->_body as $v)
      {
        if($this->_valueType =='float')
          $this->_value->add(new \Cassandra\Float($v));
        else
          $this->_value->add($v);
      }
    }

		return $this->_value;
	}

	public function jsonSerialize() : mixed
	{
		return $this->_body;
	}

	public function toArray()
	{
		return $this->_body;
	}

	public function __toString()
	{
		return json_encode($this->_body);
	}

//	public function setDefault()
//	{
//		$this->_body = (object) array();
//		return $this;
//	}


}
