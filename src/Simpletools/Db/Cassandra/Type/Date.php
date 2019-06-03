<?php

namespace Simpletools\Db\Cassandra\Type;

class Date
{
    protected $_value;

    public function __construct($seconds)
    {
        $this->_value = (string) new \Cassandra\Date($seconds);
    }

    public function value()
    {
        return $this->_value;
    }

    public function __toString()
    {
        return $this->value();
    }
}
