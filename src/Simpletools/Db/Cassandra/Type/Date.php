<?php

namespace Simpletools\Db\Cassandra\Type;

class Date implements \JsonSerializable
{
    protected $_value;

    public function __construct(\Cassandra\Date|int|float|string|null $time = null)
    {

			if(is_numeric($time) && strlen($time) <= 10)
				$this->_value = new \Cassandra\Date((int)$time);
			elseif (is_string($time) && ($time = strtotime($time)))
				$this->_value = new \Cassandra\Date($time);
			elseif ($time===null||false)
				$this->_value = new \Cassandra\Date();
			elseif ($time instanceof \Cassandra\Date)
				$this->_value = $time;
			else
				throw new \Exception("Date: Value is not a timestamp or date");
    }

		public function jsonSerialize() : mixed
		{
			return $this->_value->toDateTime()->getTimestamp();
		}

    public function value()
    {
        return $this->_value;
    }

    public function toInt()
    {
        return $this->_value->toDateTime()->getTimestamp();
    }

    public function toDateTime()
    {
        return $this->_value->toDateTime();
    }

    public function __toString()
    {
			return (string)$this->_value->toDateTime()->getTimestamp();
    }

    public function setDefault()
    {
        $this->_value =  new \Cassandra\Date(0);
        return $this;
    }
}
