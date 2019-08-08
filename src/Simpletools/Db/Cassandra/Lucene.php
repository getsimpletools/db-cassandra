<?php

namespace Simpletools\Db\Cassandra;

class Lucene
{
	public $statement;
	public $indexName;

	public function __construct($indexName,  $luceneQuery)
	{
		if(!is_object($luceneQuery) && !is_array($luceneQuery)) throw new \Exception("Lucene query must be an object or an array",400);

		$luceneQuery = json_encode($luceneQuery);

		$this->indexName = preg_replace('/[^\w]/','',(string)$indexName);
		$this->statement = $luceneQuery;
	}
}
