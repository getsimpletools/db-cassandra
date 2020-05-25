<?php
/*
 * Simpletools Framework.
 * Copyright (c) 2009, Marcin Rosinski. (https://www.getsimpletools.com)
 * All rights reserved.
 *
 * LICENCE
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - 	Redistributions of source code must retain the above copyright notice,
 * 		this list of conditions and the following disclaimer.
 *
 * -	Redistributions in binary form must reproduce the above copyright notice,
 * 		this list of conditions and the following disclaimer in the documentation and/or other
 * 		materials provided with the distribution.
 *
 * -	Neither the name of the Simpletools nor the names of its contributors may be used to
 * 		endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @framework		Simpletools
 * @copyright  		Copyright (c) 2009 Marcin Rosinski. (http://www.getsimpletools.com)
 * @license    		http://www.opensource.org/licenses/bsd-license.php - BSD
 *
 */

namespace Simpletools\Db\Cassandra;

use \Simpletools\Db\Cassandra\Doc\Body;
use Simpletools\Db\Cassandra\Type\Uuid;
use Simpletools\Db\Cassandra\Type\BigInt;
use Simpletools\Db\Cassandra\Type\Map;
use Simpletools\Db\Cassandra\Type\Date;
use Simpletools\Db\Cassandra\Type\Timestamp;

class Doc
{

	protected $_id;
	protected $_query;
	protected $_table;
	protected $_keyspace;
	protected $_columns;
	protected $_ttl;
	protected $_convertMapToJson;
	protected $_removeFromSet;

	protected $_body;

	protected $_loaded = false;
	protected $_originBody;
	protected $_lucene;
	protected $___consistency = null;


//	protected $_diff = [
//			'upsert' => [],
//			'delete' => [],
//	];

	public function __construct($id=null)
	{
		$this->_body = new Body((object) array());

		if($id !== null)
		{
			$this->_id = $id;
		}
		else
		{
			$this->_id = new Uuid();
		}
	}

	public function lucene($indexName,  $luceneQuery)
	{
			$this->_lucene = new Lucene($indexName, $luceneQuery);
			return $this;
	}

    public function consistency($consistency=null)
    {
        if($consistency===null) return $this->___consistency;

        $this->___consistency = $consistency;

        return $this;
    }

	protected function connect()
	{
		if(!$this->_table)
			throw new \Exception('Please specify table as an argument of ->table()');


		if(!$this->_query)
		{
			$this->_query = new Query($this->_table, $this->_keyspace);
		}

		if($this->_id !== null && !is_array($this->_id))
		{
			$this->_id = [
					$this->_query->getPrimaryKey()[0] => $this->_id
			];
		}

        if($this->___consistency!==null) {
            $this->_query->consistency($this->___consistency);
        }

		$this->columns($this->_columns);
	}

	public function table($table)
	{
		$this->_table = $table;
		return $this;
	}

	public function keyspace($keyspace)
	{
		$this->_keyspace = $keyspace;
		return $this;
	}

	public function columns()
	{
		$args = func_get_args();

		if(count($args) == 1)
		{
			$args = $args[0];
		}

		if($args)
		{
			$this->_columns = $args;
			if(is_string($this->_columns)) $this->_columns =  explode(',',$this->_columns);
		}

		return $this;
	}

	public function getSaveQuery()
	{
		$this->connect();

		if(is_array($this->_id))
		{
			foreach ($this->_query->getPrimaryKey() as $key)
			{
				if(isset($this->_id[$key]))
					$this->body->{$key} = $this->_id[$key];
				else
					throw new \Exception("Couldn't save the doc, missing primary key($key)",400);
			}
		}
		else
			throw new \Exception("Couldn't save the doc, missing primary key",400);


		$this->_query
				->set($this->_body)
				->expires($this->_ttl)
				->convertMapToJson($this->_convertMapToJson);

		return $this->_query;
	}

	public function save()
	{
		$this->getSaveQuery();
		$this->_query->run();
		$this->_query = null;

		return $this;
	}

	public function getUpdateQuery()
	{
		$this->connect();


		$this->_query
				->update($this->_body);

		if(is_array($this->_id))
		{
			foreach ($this->_query->getPrimaryKey() as $key)
			{
				if(isset($this->_id[$key]))
					$this->_query->filter($key,$this->_id[$key]);
				else
					throw new \Exception("Couldn't save the doc, missing primary key($key)",400);
			}
		}
		else
			throw new \Exception("Couldn't save the doc, missing primary key",400);


		$this->_query
				->expires($this->_ttl)
				->removeFromSet($this->_removeFromSet)
				->convertMapToJson($this->_convertMapToJson);

		return $this->_query;
	}

	public function update()
	{
		$this->getUpdateQuery();
		$this->_query->run();
		$this->_query = null;

		if($this->_removeFromSet)
		{
			foreach ($this->_removeFromSet as $setField => $setVal)
			{
				if(isset($this->_body->{$setField}))
				{
					$this->_body->{$setField} = array_diff($this->_body->{$setField}, $setVal);
				}
			}


		}
		return $this;
	}


	public function getLoadQuery()
	{
		$this->connect();

		if($this->_columns)
		{
			foreach ($this->_query->getPrimaryKey() as $key)
			{
				if(!in_array($key,$this->_columns))
					$this->_columns[] = $key;
			}
		}

		if(is_array($this->_id) && count($this->_id))
		{
			$keys = $this->_id;

			$this->_query->columns($this->_columns)->where(key($keys), array_shift($keys));

			foreach ($keys as $key =>$val)
			{
				$this->_query->also($key, $val);
			}

			if($this->_lucene)
				$this->_query->also($this->_lucene);

			$this->_query
					->convertMapToJson($this->_convertMapToJson)
					->limit(2);
		}
		elseif ($this->_lucene instanceof Lucene)
		{
			$this->_query->columns($this->_columns)->where($this->_lucene);
			$this->_query
					->convertMapToJson($this->_convertMapToJson)
					->limit(2);
		}

		return $this->_query;
	}

	public function load()
	{
		$this->getLoadQuery();
		$this->_query->run();

		if($this->_query->length() == 0)
		{
			$this->_query = null;
			throw new \Exception("Your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") does not exists in the '$this->_table' table", 404);
		}
		elseif($this->_query->length() > 1)
		{
			$this->_query = null;
			throw new \Exception("Unsafe loading of document your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") returns more then one document from the '$this->_table' table",400);
		}

		$this->body($this->_query->fetch());


		foreach ($this->_query->getPrimaryKey() as $k)
		{
			$this->_id[$k] = $this->_body->{$k};
		}

		$this->_lucene = null;
		$this->_query = null;

		return $this;
	}

	public function increase($data)
	{
		$this->connect();

		$this->_query
			->update($this->_body);

		if(is_array($this->_id))
		{
			foreach ($this->_query->getPrimaryKey() as $key)
			{
				if(isset($this->_id[$key]))
					$this->_query->filter($key,$this->_id[$key]);
				else
					throw new \Exception("Couldn't save the doc, missing primary key($key)",400);
			}
		}
		else
			throw new \Exception("Couldn't save the doc, missing primary key",400);


		$this->_query
			->increase($data)
			->run();

		return $this;
	}

	public function decrease($data)
	{
		$this->connect();

		$this->_query
			->update($this->_body);

		if(is_array($this->_id))
		{
			foreach ($this->_query->getPrimaryKey() as $key)
			{
				if(isset($this->_id[$key]))
					$this->_query->filter($key,$this->_id[$key]);
				else
					throw new \Exception("Couldn't save the doc, missing primary key($key)",400);
			}
		}
		else
			throw new \Exception("Couldn't save the doc, missing primary key",400);


		$this->_query
			->decrease($data)
			->run();

		return $this->_query;
	}


	public function __set($name,$value)
	{
		if($name=="body")
		{
			return $this->body($value);
		}
		else
		{
			throw new \Exception("Provided property `{$name}` doesn't exist");
		}
	}

	public function __get($name)
	{
		if($name=='body')
		{
			return !isset($this->_body) ? ($this->_body = new Body($this->_body)) : $this->_body;
		}
	}


	public function body($body=null)
	{
		if($body===null)
			return $this;

		if($body instanceof Body)
		{
			$this->_body = new Body($body);
			return $this;
		}

		$this->_body = new Body($body);

		return $this;
	}


	public function expires($seconds = null)
	{
		$this->_ttl = $seconds;
		return $this;
	}

	public function getRemoveQuery()
	{
		$this->connect();
		if($this->_id instanceof Uuid)
		{
			$this->_query->delete('id', $this->_id);
		}
		elseif(is_array($this->_id))
		{
			$keys = $this->_id;
			$this->_query->delete(key($keys), array_shift($keys));

			foreach ($keys as $key =>$val)
			{
				$this->_query->also($key, $val);
			}
		}

		return $this->_query;
	}

	public function remove()
	{
		$this->getRemoveQuery();
		$this->_query->run();
		$this->body(array());
	}


	public function convertMapToJson($boolean = true)
	{
		$this->_convertMapToJson = $boolean;
		return $this;
	}

	public function removeFromSet($data)
	{
		$this->_removeFromSet = $data;
		return $this;
	}

	public function resetQuery()
	{
		$this->_query = null;
	}


	/*
	protected function  arrayDiff($arr1, $arr2)
	{
		return array_udiff($arr1, $arr2, function($v1, $v2){
			if(is_object($v1) && is_object($v2))
			{
				return json_encode($v1) === json_encode($v2) ? 0 : -1;
			}
			elseif(	is_object($v1) || is_object($v2))
			{
				return -1;
			}
			else
			{
				return $v1 === $v2 ? 0 : -1;
			}
		});
	}

	protected function getDifference($new, $origin, $currentPath = array())
	{
		foreach ($new as $k => $v)
		{
			$path = $currentPath;
			$path[] = $k;

			if(is_object($v))
			{
				if(property_exists($origin, $k))
				{
					if(is_object($origin->{$k}))
					{
						$this->getDifference($new->{$k},$origin->{$k}, $path);
					}
					else
					{
						$this->_diff['upsert'][implode('.',$path)] =  $new->{$k};
					}
					unset($origin->{$k});
				}
				else
				{
					$this->_diff['upsert'][implode('.',$path)] = $new->{$k};
				}
			}
			elseif(is_array($v))
			{
				if(property_exists($origin, $k))
				{
					//echo"<pre>=================";var_dump($origin->{$k},$v);
					//echo"<pre>++++++++++++++++";var_dump($this->arrayDiff($v,$origin->{$k}) , $this->arrayDiff($origin->{$k},$v));

					if(!is_array($origin->{$k}) || $this->arrayDiff($v,$origin->{$k}) || $this->arrayDiff($origin->{$k},$v))
					{
						$this->_diff['upsert'][implode('.',$path)] =  $new->{$k};
					}
					unset($origin->{$k});
				}
				else
				{
					$this->_diff['upsert'][implode('.',$path)] = $new->{$k};
				}
			}
			else
			{
				if(property_exists($origin, $k))/
				{
					if(gettype ($v) != gettype($origin->{$k}) || $v != $origin->{$k})
					{
						$this->_diff['upsert'][implode('.',$path)] =  $new->{$k};
					}
					unset($origin->{$k});
				}
				else
				{
					$this->_diff['upsert'][implode('.',$path)] = $new->{$k};
				}
			}
		}

		if($origin)
		{
			foreach ($origin as $k => $v)
			{
				$path = $currentPath;
				$path[] = $k;
				$this->_diff['delete'][implode('.',$path)] = 1;
			}
		}
	}


//	public function save()
//	{
//		if($this->_loaded)
//		{
//			$this->connect();
//			$new = $this->_body->toObject();
//			$origin = $this->_originBody->toObject();
//
//			$this->getDifference($new,$origin);
//			//	$this->_diff['upsert']['engine.variants[0].capacity'] = 2;
//			//echo"<pre>";var_dump($this->_diff);die;
//
//
//			if($this->_diff['upsert'] || $this->_diff['delete'])
//			{
//				$mutateIn = $this->_bucket->mutateIn((string) $this->_id);
//
//				foreach ($this->_diff['upsert'] as $k => $v)
//				{
//					$mutateIn->upsert($k, $v, true);
//				}
//
//				foreach ($this->_diff['delete'] as $k => $v)
//				{
//					$mutateIn->remove($k);
//				}
//				if($this->expire()){
//					$mutateIn->withExpiry($this->expire());
//				}
//				$res = $mutateIn->execute();
//
//				if($res->error)
//				{
//					throw new \Exception($res->error);
//				}
//
//				$this->_diff['upsert'] = [];
//				$this->_diff['delete'] = [];
//			}
//
//			$this->_originBody = new Body(json_decode(json_encode($this->_body)));
//		}
//		else
//		{
//			$this->connect();
//
//			if($this->_ns)
//			{
//				$this->_body->_ns = $this->_ns;
//			}
//			$raw = $this->_body->toObject();
//
//
//			$res = $this->_bucket->upsert((string) $this->_id,$raw, $this->_getOptionForSave());
//
//			if($res->error)
//			{
//				throw new \Exception($res->error);
//			}
//		}
//
//		return $this;
//	}


	protected function convertToNestedBody($body, $keys, $val)
	{
		$key = array_shift($keys);
		if(!isset($body->{$key}))
		{
			$body->{$key} = new \stdClass();
		}

		if($keys)
		{
			$this->convertToNestedBody($body->{$key}, $keys, $val);
		}
		else
		{
			$body->{$key} = $val;
		}
	}


//	public function to2d(){
//
//		$input = array(
//				'meta'  => $this->_meta->toObject(),
//				'body'  => $this->_body->toObject()
//		);
//		$ritit = new \RecursiveIteratorIterator(new \RecursiveArrayIterator($input));
//		$result = array();
//		foreach ($ritit as $leafValue) {
//			$keys = array();
//			foreach (range(0, $ritit->getDepth()) as $depth) {
//				$keys[] = $ritit->getSubIterator($depth)->key();
//			}
//			$result[ join('.', $keys) ] = $leafValue;
//		}
//
//		return $result;
//	}
	*/
}
