<?php

	namespace Simpletools\Db\Cassandra;

	class FullyQualifiedQuery
	{
		protected $_queryString = '';

		public function __construct($queryString)
		{
			$this->_queryString = $queryString;
		}

		public function __toString()
		{
			return $this->_queryString;
		}
	}