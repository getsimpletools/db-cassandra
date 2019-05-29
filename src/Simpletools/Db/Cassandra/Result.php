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

use Simpletools\Db\Cassandra\Type\Uuid;

class Result implements \Iterator
{
    protected $_result 	= '';
    protected $___connection 			= '';

    protected $_firstRowCache	= null;
    protected $_firstRowCached	= false;

    protected $_position 		= 0;
    protected $_currentRow 		= false;
    protected $_columnsMap      = array();

    public function __construct($result, $connection)
    {
        $this->_result              = $result;
        $this->___connection		= $connection;

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

    protected function _parseColumn($column,$value,$rawResultAssoc)
    {
        if(isset($this->_columnsMap[$column]))
        {
            $cast = $this->_columnsMap[$column];

            if(is_callable($cast))
            {
                $value = $this->_callReflection($cast,$rawResultAssoc);
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

    private function _callReflection($callable, array $args = array())
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
            if(isset($args[$name]))
            {
                $pass[] = $args[$name];
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
        if(!$this->_result) return 0;

        return $this->_result->count();
    }

    public function fetch()
    {
        $result = $this->_result->current();
        $this->_result->next();

        if(!$result) return false;

        foreach($result as $key => $value)
        {
            if($value instanceof \Cassandra\Uuid)
            {
                $result[$key] = new Uuid((string) $value);
            }
        }

        return $this->_parseColumnsMap($result);
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
        return $this->___connection->insert_id;
    }

    protected function _loadFirstRowCache()
    {
        if(!$this->_result) return;

        if(!$this->_firstRowCached)
        {
            $this->_firstRowCache 	= (object) $this->_result->first();
            $this->_firstRowCached 	= true;
            $this->_result->rewind();
        }
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

    public function rewind()
    {
        $this->_result->rewind();
        $this->_position 	= 0;

        if($this->_currentRow===false)
        {
            $this->_currentRow = $this->fetch();
        }
    }

    public function current()
    {
        return $this->_currentRow;
    }

    public function key()
    {
        return $this->_position;
    }

    public function next()
    {
        $this->_currentRow = $this->fetch();
        ++$this->position;
        return $this->_currentRow;
    }

    public function valid()
    {
        return ($this->_currentRow!==null) ? true : false;
    }
}