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

	protected $_body;

	protected $_loaded = false;
	protected $_originBody;


//	protected $_diff = [
//			'upsert' => [],
//			'delete' => [],
//	];

	public function __construct($id=null)
	{
		$this->_body = new Body((object) array());

		if(is_string($id))
		{
			$this->_id = new Uuid($id);
			$this->_body->id = $this->_id;
		}
		else
		{
			$this->_id = $id;
		}
	}

	protected function connect()
	{
		if(!$this->_table)
			throw new \Exception('Please specify table as an argument of ->table()');


		if(!$this->_query)
		{
			$this->_query = new Query($this->_table, $this->_keyspace);
		}

		$this->columns($this->_columns);
	}

	public function table($table)
	{
		$this->_table = $table;
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
			$this->_columns = $args;

		return $this;
	}

	public function save()
	{
		$this->connect();

		if($this->_id instanceof Uuid)
		{
			$this->_body->id = $this->_id;
		}
		elseif(is_array($this->_id))
		{
			foreach ($this->_id as $key =>$val)
			{
				$this->body->{$key} = $val;
			}
		}


		$this->_query
				->set($this->_body)
				->expires($this->_ttl)
				->convertMapToJson($this->_convertMapToJson)
				->run();
		$this->_query = null;
		//$this->load();
		return $this;
	}

	public function load()
	{
		$this->connect();


		if($this->_id instanceof Uuid)
		{
			$this->_query->get($this->_id)
					->columns($this->_columns)
					->convertMapToJson($this->_convertMapToJson)
					->limit(2)
					->run();


			if($this->_query->length() == 0)
			{
				throw new \Exception("Your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") does not exists in the '$this->_table' table");
			}
			elseif($this->_query->length() > 1)
			{
				throw new \Exception("Unsafe loading of document your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") returns more then one document from the '$this->_table' table");
			}

			$this->body($this->_query->fetch());
		}
		elseif(is_array($this->_id))
		{
			$keys = $this->_id;


			$this->_query->columns($this->_columns)->where(key($keys), array_shift($keys));

			foreach ($keys as $key =>$val)
			{
				$this->_query->also($key, $val);
			}

			$this->_query
					->convertMapToJson($this->_convertMapToJson)
					->limit(2)
					->run();

			if($this->_query->length() == 0)
			{
				throw new \Exception("Your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") does not exists in the '$this->_table' table");
			}
			elseif($this->_query->length() > 1)
			{
				throw new \Exception("Unsafe loading of document your key(". (is_array($this->_id) ? json_encode($this->_id):$this->_id).") returns more then one document from the '$this->_table' table");
			}

			$this->body($this->_query->fetch());
		}

		$this->_query = null;

		return $this;
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

	public function remove()
	{
		$this->connect();
		if($this->_id instanceof Uuid)
		{
			$this->_query->delete('id', $this->_id)
					->run();

			$this->body(array());
		}
		elseif(is_array($this->_id))
		{
			$keys = $this->_id;
			$this->_query->delete(key($keys), array_shift($keys));

			foreach ($keys as $key =>$val)
			{
				$this->_query->also($key, $val);
			}

			$this->_query->run();
			$this->body(array());
		}
	}


	public function convertMapToJson($boolean = true)
	{
		$this->_convertMapToJson = $boolean;
		return $this;
	}


//	public function loaded()
//	{
//		//$this->_loaded = true;
//		//$this->_originBody = new Body(unserialize(serialize($this->_body)));
//
//		return $this;
//	}





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



	public function remove()
	{

	}



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
