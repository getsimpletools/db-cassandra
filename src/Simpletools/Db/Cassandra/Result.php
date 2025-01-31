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

use Simpletools\Db\Cassandra\Type\BigInt;
use Simpletools\Db\Cassandra\Type\Blob;
use Simpletools\Db\Cassandra\Type\Inet;
use Simpletools\Db\Cassandra\Type\Date;
use Simpletools\Db\Cassandra\Type\Map;
use Simpletools\Db\Cassandra\Type\Timestamp;
use Simpletools\Db\Cassandra\Type\Uuid;
use Simpletools\Db\Cassandra\Type\Set;

class Result implements \Iterator
{

    /**
    * Initial Variable Values
    * 
    *
    * As of PHP 8.2 you are no longer able create dynamic properties
    * Initialise variables which were previously dynamically allocated
    * as public, as per default constructor.
    **/


    protected $_result 	= '';
    protected $_client;

    protected $_firstRowCache	= null;
    protected $_firstRowCached	= false;

    protected $_position 		= 0;
    protected $_currentRow 		= false;
    protected $_columnsMap      = array();
    protected $_schema = array();
	protected $_convertMapToJson;
	protected $_autoScroll = false;
	protected $_scroll_id = null;

    public function __construct($result, $client)
    {
        $this->_result              = $result;
        $this->_client							= $client;

        if($result === null OR $result instanceof \Cassandra\Rows)
        {
            $this->_position 	= 0;
            $this->_loadFirstRowCache();
        }
        else
            throw new Exception("Not supported result format provided, make sure its an instance of \Cassandra\Row",403);
    }


    public function mapColumns(array $columnsMap)
    {
        $this->_columnsMap = $columnsMap;

        return $this;
    }

    protected function _parseColumn($column, $value, $rawResult)
    {
        if(isset($this->_columnsMap[$column]))
        {
            $cast = $this->_columnsMap[$column];
            if(is_array($cast))
            {
                $cast   = $cast['map'];
            }

            if(is_callable($cast))
            {
                $value = $this->_callReflection($cast,$rawResult);
            }
            elseif($cast=='json' OR $cast=='json:array' OR $cast=='json:object')
            {
                $assoc = false;
                if($cast=='json:array')
                {
                    $assoc = true;
                }

                $value = json_decode($value,$assoc);
            }
            elseif(is_string($cast))
            {
                settype($value,$cast);
            }
        }

        return $value;
    }

    private function _callReflection($callable, mixed $args = null)
    {
        if(is_array($callable))
        {
            $reflection 	= new \ReflectionMethod($callable[0], $callable[1]);
        }
        elseif(is_string($callable))
        {
            $reflection 	= new \ReflectionFunction($callable);
        }
        elseif(is_a($callable, 'Closure') || is_callable($callable, '__invoke'))
        {
            $objReflector 	= new \ReflectionObject($callable);
            $reflection    	= $objReflector->getMethod('__invoke');
        }

        $pass = array();
        foreach($reflection->getParameters() as $param)
        {
            $name = $param->getName();
            if(isset($args->{$name}))
            {
                $pass[] = $args->{$name};
            }
            else
            {
                try
                {
                    $pass[] = $param->getDefaultValue();
                }
                catch(\Exception $e)
                {
                    $pass[] = null;
                }
            }
        }

        return $reflection->invokeArgs($callable, $pass);
    }

    protected function _parseColumnsMap($result)
    {
        if($result && $this->_columnsMap)
        {
            foreach($this->_columnsMap as $column => $cast)
            {
                if(isset($result->{$column}))
                {
                    $result->{$column} = $this->_parseColumn($column, $result->{$column}, $result);
                }
            }
        }

        return $result;
    }

    public function isEmpty()
    {
        if(!$this->_result) return true;

        return $this->_result->count()>0 ? false : true;
    }

    public function length()
    {
    	//echo"<pre>";var_dump($this->_result);die;
        if(!$this->_result) return 0;

        return $this->_result->count();
    }

    public function fetch()
    {
        $result = $this->_result->current();
        $this->_result->next();

				if(!$result)
				{
					if(!$this->_autoScroll) return false;

					if($this->_result->isLastPage())
						return false;


					$this->_result = $this->_client->nextPageWithReconnect($this->_result);


					$result = $this->_result->current();
					$this->_result->next();
				}

				$result = (object)$result;

        foreach($result as $key => $val)
        {
            if(is_object($val))
            {
                $result->{$key} = $this->toResultType($key,$val);
            }
           // $result[$key] = $this->_parseCell($value);
        }

        //return $result;
        return $this->_parseColumnsMap($result);
    }

    public function setSchema($schema)
		{
			$this->_schema = $schema;
		}

    public function fetchAll($returnObject=true)
    {
        if($this->isEmpty()) return array();

        $datas = array();
        while($data = $this->fetch())
        {
            $datas[] = $data;
        }

        $this->free();
        return $datas;
    }

    public function &getRawResult()
    {
        return $this->_result;
    }

    public function free()
    {
        //$this->_result = '';
    }

    public function __desctruct()
    {
			//$this->_result = '';
    }

    public function getAffectedRows()
    {
        return $this->_result->count();
    }

    public function getInsertedId()
    {
        return $this->_client->connector()->insert_id;
    }

    protected function _loadFirstRowCache()
    {
        if(!$this->_result) return;

        if(!$this->_firstRowCached)
        {
            $this->_firstRowCache 	= $this->_result->first();
            $this->_firstRowCached 	= true;

            if($this->_firstRowCache)
            {
                foreach ($this->_firstRowCache as $key => $value) {
                    $this->_firstRowCache[$key] = $this->_parseCell($value);
                }

                $this->_firstRowCache = (object)$this->_firstRowCache;
            }

            $this->_result->rewind();
        }
    }

    protected function _parseCell($value)
    {
        if($value instanceof \Cassandra\Uuid)
        {
            return new Uuid((string) $value);
        }
        elseif($value instanceof \Cassandra\BigInt)
        {
            return new BigInt((string) $value);
        }

        return $value;
    }


    public function getFirstRow()
    {
        return $this->_firstRowCache;
    }

    public function __get($name)
    {
        //$this->_loadFirstRowCache();
        return isset($this->_firstRowCache->{$name}) ? $this->_firstRowCache->{$name} : null;
    }

    public function __isset($name)
    {
        //$this->_loadFirstRowCache();
        return isset($this->_firstRowCache->{$name});
    }

    public function rewind() : void
    {
        $this->_result->rewind();
        $this->_position 	= 0;

        if($this->_currentRow===false)
        {
            $this->_currentRow = $this->fetch();
        }
    }

    public function current() : mixed
    {
        return $this->_currentRow;
    }

    public function key() : mixed
    {
        return $this->_position;
    }

    public function next() : void
    {
        $this->_currentRow = $this->fetch();
        ++$this->_position;
        // return $this->_currentRow;
    }

    public function valid() : bool
    {
        return ($this->_currentRow) ? true : false;
    }


	protected function toResultType($key, $value)
	{
	    //to enable column1 as column2
	    if(isset($this->_columnsMap[$key]) && !is_callable($this->_columnsMap[$key]) && isset($this->_columnsMap[$key]['origin']))
	        $key = $this->_columnsMap[$key]['origin'];

	    if(isset($this->_schema[$key]))
		{
			if($this->_schema[$key] == 'int') return (int)0;
			elseif (substr($this->_schema[$key],0,3)== 'map')
			{
				$types = explode(',',str_replace(['map','<','>',' ',],'',$this->_schema[$key]));

				return (new Map($value, $types[0], $types[1], $this->_convertMapToJson))->toObject();
			}
			elseif (substr($this->_schema[$key],0,3)== 'set')
			{
				$types = explode(',',str_replace(['set','<','>',' ',],'',$this->_schema[$key]));

				return (new Set($value, $types[0]))->toArray();
			}
			elseif($this->_schema[$key] == 'double') 			return (float)$value;
			elseif($this->_schema[$key] == 'boolean') 		return $value;
			elseif($this->_schema[$key] == 'text') 				return '';
			elseif($this->_schema[$key] == 'uuid') 				return (string)$value;
			elseif($this->_schema[$key] == 'timestamp') 	return $value === null  ? (new Timestamp($value))->setDefault()->toInt() : (new Timestamp($value))->toInt();
			elseif($this->_schema[$key] == 'decimal') 		return (float)$value;
			elseif($this->_schema[$key] == 'float') 			return (float)$value;
			elseif($this->_schema[$key] == 'bigint') 			return (string)($value);
			elseif($this->_schema[$key] == 'tinyint') 		return (int)($value);
			elseif($this->_schema[$key] == 'counter') 		return (int)($value);
			elseif($this->_schema[$key] == 'date') 				return $value === null  ? (new Date($value))->setDefault()->toInt() : (new Date($value))->toInt();
			elseif($this->_schema[$key] == 'timeuuid') 		return (string)($value);
			elseif($this->_schema[$key] == 'blob') 				return (new Blob($value))->getContent();
            elseif($this->_schema[$key] == 'inet') 				return (string)$value;
			else
				throw new \Exception("Your key($key) using unsupported data type");
		}
		else
			throw new \Exception("Your key($key) is missing in table schema");

	}

	public function convertMapToJson($boolean = true)
	{
		$this->_convertMapToJson = $boolean;
		return $this;
	}

	public function autoScroll()
	{
		$this->_autoScroll = true;
		return $this;
	}

	public function getScrollId()
	{
		return base64_encode($this->_result->pagingStateToken());
	}
}
