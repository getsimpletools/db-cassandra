<?php

namespace Simpletools\Db\Cassandra;

use Simpletools\Db\Cassandra\Type\AutoIncrement;
use Simpletools\Db\Cassandra\Type\BigInt;
use Simpletools\Db\Cassandra\Type\Blob;
use Simpletools\Db\Cassandra\Type\Date;
use Simpletools\Db\Cassandra\Type\Decimal;
use Simpletools\Db\Cassandra\Type\Timestamp;
use Simpletools\Db\Cassandra\Type\Timeuuid;
use Simpletools\Db\Cassandra\Type\SimpleFloat;
use Simpletools\Db\Cassandra\Type\Tinyint;
use Simpletools\Db\Cassandra\Type\Uuid;
use Simpletools\Db\Cassandra\Type\Map;

class Query implements \Iterator
{
    //https://datastax.github.io/php-driver/api/class.Cassandra/
    const CONSISTENCY_ANY           = 0;
    const CONSISTENCY_ONE           = 1;
    const CONSISTENCY_TWO           = 2;
    const CONSISTENCY_THREE         = 3;
    const CONSISTENCY_QUORUM        = 4;
    const CONSISTENCY_ALL           = 5;
    const CONSISTENCY_LOCAL_QUORUM  = 6;
    const CONSISTENCY_EACH_QUORUM   = 7;
    const CONSISTENCY_SERIAL        = 8;
    const CONSISTENCY_LOCAL_SERIAL  = 9;
    const CONSISTENCY_LOCAL_ONE     = 10;


    protected $_query 	    = array();
    protected $_columnsMap  = array();
    protected $_client;

    protected $_result      = null;
    protected $_schema      = array();

    public function __construct($table,$keyspace=null)
    {
        $this->table($table);

        $this->_client = new Client();

        if($keyspace)
            $this->keyspace($keyspace);
        else
        {
            $keyspace = $this->_client->keyspace();
            if($keyspace)
            {
                $this->keyspace($keyspace);
            }
        }
    }

    public function client($client)
    {
        if (!($client instanceof Client))
        {
            throw new \Exception("Provided client is not an instance of \Simpletools\Db\Cassandra\Client", 404);
        }

        $this->_client = $client;

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
        	$this->_query['columns'] = $args;

        return $this;
    }

    protected $_currentJoinIndex = 0;

    public function join($tableName,$direction='left')
    {
        $tableType = 'table';

        if($tableName instanceof Table)
        {
            $tableName = '('.$tableName->getQuery().')';
            $tableType = 'query';
        }

        $this->_query['join'][$this->_currentJoinIndex] = [
            'tableType'		=> $tableType,
            'table'			=> $tableName,
            'direction'		=> $direction
        ];

        return $this;
    }

    public function leftJoin($tableName)
    {
        return $this->join($tableName,'left');
    }

    public function rightJoin($tableName)
    {
        return $this->join($tableName,'right');
    }

    public function innerJoin($tableName)
    {
        return $this->join($tableName,'inner');
    }

    protected function _on($args,$glue='')
    {
        if(
            $args instanceof Sql OR
            $args instanceof Json
        )
        {
            $this->_query['join'][$this->_currentJoinIndex]['on'] = (string) $args;
        }
        else
        {
            $operand 	= '=';
            $left 		= $args[0];

            if(count($args)>2)
            {
                $operand 	= $args[1];
                $right 		= $args[2];
            }
            else
            {
                $right 		= $args[1];
            }

            if($glue)
            {
                $this->_currentJoinIndex--;
                $glue = ' '.$glue.' ';
            }
            else
            {
                $this->_query['join'][$this->_currentJoinIndex]['on'] = '';
            }

            $this->_query['join'][$this->_currentJoinIndex]['on'] .= $glue.$left.' '.$operand.' '.$right;
        }

        $this->_currentJoinIndex++;

        return $this;
    }

    public function on()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        $this->_on($args,'');

        return $this;
    }

    public function orOn()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        $this->_on($args,'OR');

        return $this;
    }

    public function andOn()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        $this->_on($args,'AND');

        return $this;
    }

    public function using()
    {
        $this->_currentJoinIndex++;

        return $this;
    }

    public function keyspace($keyspace)
    {
    		if($keyspace)
        	$this->_query['db'] = $keyspace;

        return $this;
    }

    public function group()
    {
        $args = func_get_args();

        if(count($args) == 1)
        {
            $args = $args[0];
        }

        $this->_query['groupBy'] = $args;

        return $this;
    }

    public function sort()
    {
        $args = func_get_args();

        if(count($args) == 1)
        {
            $args = $args[0];
        }

        $this->_query['sort'] = $args;

        return $this;
    }

    public function delete()
    {
        $this->_query['type'] = "DELETE FROM";

        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        if(!count($args))
        {
            throw new \Exception('Please specify where condition as an argument of ->delete() otherwise use ->truncate()');
        }

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function set($data)
    {
        $this->_query['type'] = "INSERT";
        $this->_query['data'] = $data;

        return $this;
    }

    public function insertIgnore($data)
    {
        $this->_query['type']   = "INSERT";
        $this->_query['data']   = $data;

        $this->_query['ifNotExists'] = true;

        return $this;
    }

    public function onDuplicate($data)
    {
        $this->_query['onDuplicateData'] = $data;

        return $this;
    }

    public function update($data)
    {
        $this->_query['type'] = "UPDATE";
        $this->_query['data'] = $data;

        return $this;
    }

    public function replace($data)
    {
        $this->_query['type'] = "REPLACE";
        $this->_query['data'] = $data;

        return $this;
    }

    public function replaceDelayed($data)
    {
        $this->_query['type'] = "REPLACE DELAYED";
        $this->_query['data'] = $data;

        return $this;
    }

    public function replaceLowPriority($data)
    {
        $this->_query['type'] = "REPLACE LOW_PRIORITY";
        $this->_query['data'] = $data;

        return $this;
    }

    protected $___options = array();
    public function options($options=array())
    {
        $this->___options = $options;

        return $this;
    }


    public function getSchema()
		{
			if($this->_schema) return $this->_schema;

			$schema   = $this->_client->connector()->schema();
			$table   = $schema->keyspace($this->_query['db'])->table($this->_query['table']);

			//echo"<pre>";var_dump($table->primaryKey()[0]->name(),$table->partitionKey()[0]->name(),$table->clusteringKey(), $table->clusteringOrder());die;

			foreach ($table->columns() as $column)
			{
				$this->_schema[$column->name()] = (string)$column->type();
			}

			return $this->_schema;
		}

		protected function toSchemaType($key, $value)
		{
			if(isset($this->_schema[$key]))
			{
				if($this->_schema[$key] == 'int')
				{
					if(!is_numeric($value))
						throw new \Exception("Your key($key) is not numeric");

					return (int)$value;
				}
				elseif (substr($this->_schema[$key],0,3)== 'map')
				{
					$types = explode(',',str_replace(['map','<','>',' ',],'',$this->_schema[$key]));
					return new Map($value, $types[0], $types[1]);
				}
				elseif($this->_schema[$key] == 'double')
				{
					if(!is_numeric($value))
						throw new \Exception("Your key($key) is not numeric");

					return (float)$value;
				}
				elseif($this->_schema[$key] == 'boolean')
				{
					if(!is_bool($value))
						throw new \Exception("Your key($key) is not a boolean");

					return (bool)$value;
				}
				elseif($this->_schema[$key] == 'text') 				return (string)$value;
				elseif($this->_schema[$key] == 'uuid') 				return new Uuid($value);
				elseif($this->_schema[$key] == 'timestamp') 	return new Timestamp($value);
				elseif($this->_schema[$key] == 'decimal') 		return new Decimal($value);
				elseif($this->_schema[$key] == 'float') 			return new SimpleFloat($value);
				elseif($this->_schema[$key] == 'bigint') 			return new BigInt($value);
				elseif($this->_schema[$key] == 'tinyint') 		return new Tinyint($value);
				elseif($this->_schema[$key] == 'date') 				return new Date($value);
				elseif($this->_schema[$key] == 'timeuuid') 		return new Timeuuid($value);
				elseif($this->_schema[$key] == 'blob') 				return new Blob($value);
				else
					throw new \Exception("Your key($key) using unsupported data type");
			}
			else
				throw new \Exception("Your key($key) is missing in table schema");

		}

    protected function toSchema()
		{
			$this->getSchema();
			if(isset($this->_query['data']))
			{
				foreach ($this->_query['data'] as $key => $val)
				{
					if(!is_object($val) || $val instanceof \stdClass)
					{
						$this->_query['data'][$key] = $this->toSchemaType($key,$val);
					}
				}
			}
		}

    public function run($options=array())
    {
        if($this->_result) return $this;
        $query = $this->getQuery(true);

        if(!$options)
        {
            $options = $this->___options;
        }

        $this->_result =
            $this->_client
                ->queryOptions($options)
                ->prepare($query['preparedQuery'])
                ->execute($query['arguments']);


				$this->_result->setSchema($this->_schema);
				$this->_result->mapColumns($this->_columnsMap);

        return $this;
    }

    public function get($id,$column='id')
    {
        $this->_query['type']		= "SELECT";
        $this->_query['where'][] 	= array($column,$id);

        return $this;
        //return $this->run();
    }

    public function _escape($value)
    {
        if($value instanceof Cql)
        {
            return (string) $value;
        }
        elseif(is_float($value) || is_integer($value))
        {
            return $value;
        }
        elseif(
            $value instanceof Uuid OR
            $value instanceof Timeuuid OR
						$value instanceof Map
        )
        {
            return (string) $value;
        }
        elseif(
            $value instanceof AutoIncrement
        )
        {
            return $value->value();
        }
        elseif(
            $value instanceof Timestamp
        )
        {
            return "'".$value->toDateTime()->format(DATE_ATOM)."'";
        }
        elseif(is_bool($value))
        {
            return (int) $value;
        }
        elseif(is_null($value))
        {
            return null;
        }
        else
        {
            return "'".$this->_client->escape($value)."'";
        }
    }

    private function _prepareQuery($query, array $args)
    {
        foreach($args as $arg)
        {
            if(is_string($arg))
            {
                if(strpos($arg,'?') !== false)
                {
                    $arg = str_replace('?','<--SimpleMySQL-QuestionMark-->',$arg);
                }

                $arg = $this->_escape($arg);
            }
            elseif(
                $arg instanceof Sql OR
                $arg instanceof Json
            )
            {
                $arg = (string) $arg;
            }

            if($arg === null)
            {
                $arg = 'NULL';
            }

            $query = $this->replace_first('?', $arg, $query);
        }

        if(strpos($query,'<--SimpleMySQL-QuestionMark-->') !== false)
        {
            $query = str_replace('<--SimpleMySQL-QuestionMark-->','?',$query);
        }

        return $query;
    }

    public function replace_first($needle , $replace , $haystack)
    {
        $pos = strpos($haystack, $needle);

        if ($pos === false)
        {
            // Nothing found
            return $haystack;
        }

        return substr_replace($haystack, $replace, $pos, strlen($needle));
    }

    public function getQuery($runtime=false)
    {
				$this->toSchema();
        $args = [];

        if(!isset($this->_query['type']))
            $this->_query['type']		= "SELECT";

        if(!isset($this->_query['columns']))
        {
            $this->_query['columns']		= "*";
        }

        if(!is_array($this->_query['columns']) && !(
                $this->_query['columns'] instanceof Sql
            ))
        {
            $this->_query['columns'] = explode(',',$this->_query['columns']);
        }
        elseif(is_array($this->_query['columns']))
        {
            foreach($this->_query['columns'] as $idx => $column)
            {
                if(!is_integer($idx))
                {
                    $this->_columnsMap[$idx]      = $column;
                    $column                       = $idx;
                }
								elseif($column instanceof Map
										|| $column instanceof BigInt
										|| $column instanceof Timestamp
										|| $column instanceof Uuid
										|| $column instanceof Timeuuid
										|| $column instanceof Date
										|| $column instanceof Tinyint
										|| $column instanceof Decimal
										|| $column instanceof SimpleFloat
										|| $column instanceof Blob
								){
									$this->_query['columns'][$idx] = $column->value();
								}
                elseif($column instanceof Sql){
                    $this->_query['columns'][$idx] = (string) $column;
                }
                else
                {
                    $column = str_replace(' as ',' ',$column);

                    if(strpos($column,' ')!==false)
                    {
                        $column = explode(' ',$column);
                        foreach($column as $_columnKey => $_columnName)
                        {
                            $column[$_columnKey] = $this->escapeKey($_columnName);
                        }

                        if(isset($this->_columnsMap[$idx]))
                            $this->_columnsMap[$_columnName]      = $this->_columnsMap[$idx];

                        $this->_query['columns'][$idx] = implode(' ',$column);
                    }
                    else {
                        $this->_query['columns'][$idx] = $this->escapeKey($column);
                    }
                }
            }
        }

        $query 		= array();
        $query[] 	= $this->_query['type'];

        if($this->_query['type']=='SELECT')
        {
            $query[] = is_array($this->_query['columns']) ? implode(', ',$this->_query['columns']) : $this->_query['columns'];
            $query[] = 'FROM';
        }
        elseif(
            $this->_query['type']=='INSERT'
        )
        {
            $query[] = 'INTO';
        }

        if(strpos($this->_query['table'],'.')===false)
        {
            if(!isset($this->_query['db']))
            {
                $this->_query['db'] = $this->_client->keyspace();
                if(!$this->_query['db'])
                {
                    throw new \Exception("Please set your Database name under connect settings or using ->keyspace", 1);
                }
            }

            $query[] = $this->escapeKey($this->_query['db']).'.'.$this->escapeKey($this->_query['table']);
        }
        else
        {
            $query[] = $this->escapeKey($this->_query['table']);
        }


        if(isset($this->_query['as']))
        {
            $query[] = 'as '.$this->escapeKey($this->_query['as']);
        }

//        if(isset($this->_query['join']))
//        {
//            foreach($this->_query['join'] as $join)
//            {
//                $db = isset($join['db']) ? $join['db'] : $this->_client->getCurrentDb();
//
//                if(strpos($join['table'],'.')===false)
//                {
//                    $syntax 	= strtoupper($join['direction']).' JOIN '.$this->escapeKey($db.'.'.$join['table']);
//                }
//                else
//                {
//                    $syntax 	= strtoupper($join['direction']).' JOIN '.$this->escapeKey($join['table']);
//                }
//
//                if(isset($join['as']))
//                {
//                    $syntax .= ' as '.$join['as'];
//                }
//
//                if(isset($join['on']))
//                {
//                    $syntax .= ' ON ('.$join['on'].')';
//                }
//                elseif(isset($join['using']))
//                {
//                    $syntax .= ' USING ('.$join['using'].')';
//                }
//
//                $query[] 	= $syntax;
//            }
//        }

        $setTypes = array(
            'UPDATE' 				=> 1,
            'REPLACE' 				=> 1,
            'REPLACE DELAYED'		=> 1,
            'REPLACE LOW_PRIORITY'	=> 1
        );

        if(isset($setTypes[$this->_query['type']]))
        {
            $query[] = 'SET';

            $set = array();

            foreach($this->_query['data'] as $key => $value)
            {
                if(is_null($value))
                {
                    $set[] = $this->escapeKey($key).' = NULL';
                }
                elseif($value instanceof Cql)
                {
                    $set[] = $this->escapeKey($key).' = '.(string) $value;
                }
                else
                {
                    //$set[] = $this->escapeKey($key).' = '.$this->_escape($value);
                    $set[]  = $this->escapeKey($key).' = ?';
                    $args[] = $value;
                }
            }

            $query[] = implode(', ',$set);
        }

        $insertTypes = array(
            'INSERT' 				=> 1
        );

        if(isset($insertTypes[$this->_query['type']]))
        {
            $set    = array();
            $keys   = [];
            $values = [];

            foreach($this->_query['data'] as $key => $value)
            {
                $keys[]     = $this->escapeKey($key);
                $values[]   = ' ?';

								if($value instanceof Map
										|| $value instanceof BigInt
										|| $value instanceof Timestamp
										|| $value instanceof Uuid
										|| $value instanceof Timeuuid
										|| $value instanceof Date
										|| $value instanceof Tinyint
										|| $value instanceof Decimal
										|| $value instanceof SimpleFloat
										|| $value instanceof Blob
								){
									$args[] = $value->value();
								}
                elseif(is_null($value))
                {
                    //$set[] = $this->escapeKey($key).' = NULL';
                    $args[] = 'NULL';
                }
                else
                {
                    //$set[] = $this->escapeKey($key).' = '.$this->_escape($value);
                    //$set[]  = $this->escapeKey($key).' = ?';

                    $args[] = $value;
                }
            }

            $ttl = '';
            $ifNotExists = '';
            if($this->_ttl)
            {
                $ttl = ' USING TTL '.$this->_ttl;
            }

            if(isset($this->_query['ifNotExists']))
            {
                $ifNotExists = 'IF NOT EXISTS';
            }

            $set[] = '('.implode(', ',$keys).') VALUES('.implode(',',$values).' ) '.$ifNotExists.' '.$ttl;

            $query[] = implode(', ',$set);
        }

//        if(isset($this->_query['onDuplicateData']))
//        {
//            $query[] = 'ON DUPLICATE KEY UPDATE';
//
//            $set = array();
//
//            foreach($this->_query['onDuplicateData'] as $key => $value)
//            {
//                if(is_null($value))
//                {
//                    $set[] = $this->escapeKey($key) . ' = NULL';
//                }
//                elseif($value instanceof Json)
//                {
//                    $value->setDataSourceOut($key);
//                    $set[] = $this->_escape($value);
//                }
//                else
//                {
//                    //$set[] = $this->escapeKey($key) . ' = ' . $this->_escape($value);
//                    $set[]  = $this->escapeKey($key).' = ?';
//                    $args[] = $value;
//                }
//
//            }
//
//            $query[] = implode(', ',$set);
//        }

        if(isset($this->_query['where']))
        {
            $query['WHERE'] = 'WHERE';

            if(is_array($this->_query['where']))
            {
                foreach($this->_query['where'] as $operands)
                {
                    if(!isset($operands[2]))
                    {
                        if($operands[1]===null) {
                            $query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " IS NULL";
                        }
                        else{
                            //$query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " = " . $this->_escape($operands[1]);
                            $query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " = ?";
                            $args[] = $operands[1];
                        }

                    }
                    else
                    {
                        $operands[1] = strtoupper($operands[1]);

                        if($operands[1] == "IN" AND is_array($operands[2]))
                        {
                            $operands_ = array();

                            foreach($operands[2] as $op)
                            {
                                //$operands_[] = $this->_escape($op);
                                $operands_[]    = ' ?';
                                $args[] =        $op;
                            }

                            $query[] = @$operands[-1].' '.$this->escapeKey($operands[0])." ".$operands[1]." (".implode(",",$operands_).')';
                        }
                        else
                        {
                            if($operands[2]===null) {
                                $query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " " . $operands[1] . " NULL";
                            }
                            else
                            {
                                //$query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " " . $operands[1] . " " . $this->_escape($operands[2]);
                                $query[] = @$operands[-1] . ' ' . $this->escapeKey($operands[0]) . " " . $operands[1] . " ?";
                                $args[] = $operands[2];

                            }

                        }
                    }
                }
            }
            else
            {
                //$query[] = 'id = '.$this->_escape($this->_query['where']);
                $query[]    = 'id = ?';
                $args[]     = $this->_query['where'];
            }
        }

        if(isset($this->_query['whereSql']))
        {
            if(!isset($query['WHERE'])) $query['WHERE'] = 'WHERE';

            if($this->_query['whereSql']['vars'])
            {
                $query[] = $this->_prepareQuery($this->_query['whereSql']['statement'],$this->_query['whereSql']['vars']);
            }
            else
            {
                $query[] = $this->_query['whereSql']['statement'];
            }
        }

        if(isset($this->_query['groupBy']))
        {
            $query[] = 'GROUP BY';

            if(!is_array($this->_query['groupBy']))
            {
                $query[] = $this->_query['groupBy'];
            }
            else
            {
                $groupBy = array();

                foreach($this->_query['groupBy'] as $column)
                {
                    $groupBy[] = $column;
                }

                $query[] = implode(', ',$groupBy);
            }
        }

        if(isset($this->_query['sort']))
        {
            $query[] = 'ORDER BY';

            if(!is_array($this->_query['sort']))
            {
                $query[] = $this->_query['sort'];
            }
            else
            {
                $sort = array();

                foreach($this->_query['sort'] as $column)
                {
                    $sort[] = $column;
                }

                $query[] = implode(', ',$sort);
            }
        }

        if(isset($this->_query['limit']))
        {
            $query[] = 'LIMIT '.$this->_query['limit'];
        }

        if(isset($this->_query['offset']))
        {
            $query[] = 'OFFSET '.$this->_query['offset'];
        }


        $this->_query = array(
        		'db' => $this->_query['db'],
						'table' => $this->_query['table'],
				);

        $query = implode(' ',$query);

        if(!$runtime)
        {
            $parsedQuery = $query;
            $index = 0;


            while(strpos($parsedQuery,' ?')!==false)
            {
                $parsedQuery = $this->str_replace_first(' ?',' '.$index.'?',$parsedQuery);
                $index++;
            }

            foreach($args as $index => $arg)
            {
                $parsedQuery = str_replace($index.'?',$this->_escape($arg),$parsedQuery);
            }

            return (string) new FullyQualifiedQuery($parsedQuery);
        }


        return [
            'preparedQuery'     => (string) new FullyQualifiedQuery($query),
            'arguments'         => $args
        ];
    }

    public function str_replace_first($from, $to, $content)
    {
        $from = '/'.preg_quote($from, '/').'/';

        return preg_replace($from, $to, $content, 1);
    }

    /*
    * Prevent SQL Injection on database name, table name, field names
    */
    public function escapeKey($key)
    {
        if(
            $key instanceof Sql
        )
        {
            return (string) $key;
        }
        elseif(trim($key)=='*')
        {
            return '*';
        }
        elseif(strpos($key,'.')===false)
        {
            return $key;
        }
        else
        {
            $keys = explode('.',$key);
            foreach($keys as $index => $key)
            {
                $keys[$index] = $key;
            }

            return implode('.',$keys);
        }
    }

    public function &whereSql($statement,$vars=null)
    {
        $this->_query['whereSql'] = array('statement'=>$statement,'vars'=>$vars);

        return $this;
    }

    public function &truncate()
    {
        $this->_query['type']		= "TRUNCATE";

        return $this;
    }

    protected $_ttl;

    public function ttl($seconds=null)
    {
        if($seconds!==null)
				{
					$this->_ttl = (int) $seconds;
				}

        return $this;
    }

    public function expires($seconds)
    {
        return $this->ttl($seconds);
    }

    public function &select($columns)
    {
        $this->_query['type']		= "SELECT";
        $this->_query['columns']	= $columns;

        return $this;
    }

    public function &offset($offset)
    {
        $this->_query['offset'] 	= $offset;

        return $this;
    }

    public function &limit($limit)
    {
        $this->_query['limit'] 		= $limit;

        return $this;
    }

    public function &find()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function &filter()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        if(isset($this->_query['where']))
        {
            $args[-1] 	= 'AND';
        }

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function &where()
    {
        $args = func_get_args();
        if(count($args)==1) $args = $args[0];

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function &alternatively()
    {
        $args = func_get_args();

        $args[-1] 	= 'OR';
        $args[0] 	= $args[0];

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function &also()
    {
        $args = func_get_args();

        $args[-1] 	= 'AND';
        $args[0] 	= $args[0];

        $this->_query['where'][] 	= $args;

        return $this;
    }

    public function &table($table)
    {
        $this->_query['table'] 		= $table;

        return $this;
    }

    public function &aka($as)
    {
        if(!isset($this->_query['join']))
            $this->_query['as'] 									= $as;
        else
            $this->_query['join'][$this->_currentJoinIndex]['as'] 	= $as;

        return $this;
    }

    /*
    * AUTO RUNNNERS
    */
    public function __get($name)
    {
        $this->run();
        return $this->_result->{$name};
    }

    public function getAffectedRows()
    {
        $this->run();
        return $this->_result->getAffectedRows();
    }

    public function getInsertedId()
    {
        $this->run();
        return $this->_result->getInsertedId();
    }

    public function isEmpty()
    {
        $this->run();
        return $this->_result->isEmpty();
    }

    public function fetch()
    {
        $this->run();
        return $this->_result->fetch();
    }

    public function getFirstRow()
    {
        $this->run();
        return $this->_result->getFirstRow();
    }

    public function fetchAll()
    {
        $this->run();
        return $this->_result->fetchAll();
    }

    public function length()
    {
        $this->run();
        return $this->_result->length();
    }

    public function rewind()
    {
        $this->run();
        $this->_result->rewind();
    }

    public function current()
    {
        return $this->_result->current();
    }

    public function key()
    {
        return $this->_result->key();
    }

    public function next()
    {
        return $this->_result->next();
    }

    public function valid()
    {
        return $this->_result->valid();
    }

    public function __toString()
    {
        return $this->getQuery();
    }

}
