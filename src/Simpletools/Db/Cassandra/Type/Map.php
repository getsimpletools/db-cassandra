<?php

namespace Simpletools\Db\Cassandra\Type;

class Map implements \JsonSerializable
{
	protected $_body;
	protected $_value;
	protected $_keyType;
	protected $_valueType;

	//protected $type;
	//protected $keys;
	//protected $values;

	public function __construct($body, $keyType = \Cassandra::TYPE_TEXT, $valueType = \Cassandra::TYPE_TEXT)
	{
		if($body === null) 					$body = (object) array();
		elseif (is_string($body)) 	$body = json_decode($body);
		elseif (is_array($body)) 		$body = (object)$body;

		if ($body instanceof \Cassandra\Map)
		{
			$this->_body = new \stdClass();
			$this->_keyType = $body->type()->keyType();
			$this->_valueType = $body->type()->valueType();

			if($this->_keyType =='int') // map<int,text> can be use as an array
			{
				$this->_body = array();
				foreach (array_combine($body->keys(), $body->values()) as $key => $v)
				{
					if (substr((string)$v, 0, 1) == '{' && ($obj = json_decode($v)))
						$this->_body[$key] = $obj;
					else
						$this->_body[$key] = $v;
				}
			}
			else
			{
				foreach (array_combine($body->keys(), $body->values()) as $key => $v)
				{
					if (substr((string)$v, 0, 1) == '{' && ($obj = json_decode($v)))
						$this->_body->{$key} = $obj;
					else
						$this->_body->{$key} = $v;
				}
			}
		}
		else
		{
			$this->_body = $keyType == 'int' ? (array)$body : $body;
			$this->_keyType = $keyType;
			$this->_valueType = $valueType;
		}
	}


	public function value()
	{
		$this->_value = new \Cassandra\Map($this->_keyType, $this->_valueType);

		if($this->_body)
		{
			if($this->_keyType =='int')  // map<int,text> can be use as an array
			{
				foreach ($this->_body  as $k =>$v)
				{
					if (is_numeric($k)) $k = (int)$k;
					if (is_object($v) || is_array($v)) $v = json_encode($v);
					$this->_value->set($k, $v);
				}

			}
			else
			{
				foreach ($this->_body  as $k =>$v)
				{
					if(is_object($v) || is_array($v)) $v = json_encode($v);
					$this->_value->set($k,$v);
				}
			}
		}

		return $this->_value;
	}

	public function jsonSerialize()
	{
		return $this->_body;
	}

	public function toObject()
	{
		return $this->_body;
	}

	public function __toString()
	{
		return json_encode($this->_body);
	}

	public function __isset($name)
	{
		return isset($this->_body->{$name});
	}

	public function __set($name,$value)
	{
		if(is_object($this->_body))
			$this->_body->{$name} = &$value;
		else
			$this->_body = $value;
	}

	public function __unset($name)
	{
		unset($this->_body->{$name});
	}

	public function &__get($name)
	{
		if(property_exists($this->_body, $name))
		{
			return $this->_body->{$name};
		}
		else
		{
			return null;
		}
	}

	public function setDefault()
	{
		$this->_body = (object) array();
		return $this;
	}


}
