<?php

namespace Simpletools\Db\Cassandra;

class Model extends Client
{
    protected static $____selfModel;
    protected $___cluster;
    protected $___keyspace;

    public function __construct($cluster=null)
    {
        $this->___cluster 	= defined('static::CLUSTER') ? static::CLUSTER : $cluster;

        parent::__construct($this->___cluster);

        $this->___keyspace 	= defined('static::KEYSPACE') ? static::KEYSPACE : $this->___keyspace;
    }

    public static function self()
    {
        if(isset(static::$____selfModel[static::class]))
            return static::$____selfModel[static::class];

        $obj = new static();

        if(method_exists($obj, 'init') && is_callable(array($obj,'init')))
        {
            call_user_func_array(array($obj,'init'),func_get_args());
        }

        return static::$____selfModel[static::class]   = $obj;
    }

    public function table($table)
    {
        $args 	= func_get_args();
        $table 	= array_shift($args);

        $query = new Query($table);
        $query->keyspace($this->___keyspace);

        return $query;
    }

		public function doc($id =null)
		{
			return new Doc($id);
		}
}
