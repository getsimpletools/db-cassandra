<?php

namespace Simpletools\Db\Cassandra;

class Cql
{
	protected $_statement = '';

	public function __construct($statement)
	{
		$this->_statement = $statement;
	}

	public function __toString()
	{
		return $this->_statement;
	}
}