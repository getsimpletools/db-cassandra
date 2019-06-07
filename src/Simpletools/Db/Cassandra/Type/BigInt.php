<?php

namespace Simpletools\Db\Cassandra\Type;

class BigInt implements \JsonSerializable
{
    protected $_value;

    public function __construct($bigint)
    {
			if(is_numeric($bigint))
				$this->_value = new \Cassandra\BigInt((string) $bigint);
			elseif($bigint instanceof \Cassandra\BigInt)
				$this->_value = $bigint;
			else
				throw new \Exception("BigInt: Value is not numeric");
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
        return (string) $this->value();
    }

    public function toInt()
    {
        return (int) (string) $this->value();
    }

    public function setDefault()
		{
			$this->_value = new \Cassandra\BigInt('0');
			return $this;
		}
}
