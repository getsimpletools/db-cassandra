<?php

namespace Simpletools\Db\Cassandra;

class TempTable
{
    protected static $_schemas                  = [];
    protected static $_registeredTables         = [];
    protected static $_registerMaxSize          = 0;
    protected static $_shutdown                 = false;

    protected $schema                           = '';
    protected $thisTableName                    = '';
    protected $_appendix                        = '';
    protected $_thisCreated                     = false;
    protected $_keep                            = false;

    protected $_schema;

    /*
     * STATIC::
     */
    public static function schema($name)
    {
        self::$_schemas[$name] = $schema = new Schema($name);
        return $schema;
    }

    /*
     * Usually run once only
     */
    public static function listActiveTempTables($keyspace)
    {
        $tablesQ = (new Query('tables','system_schema'))
            ->where('keyspace_name',$keyspace);

        $tables = [];

        foreach($tablesQ as $table)
        {
            if(strpos($table->table_name,'tmp_')!==0) continue;

            $index = $table->keyspace_name.'.'.$table->table_name;

            $meta_ = explode('_',$table->table_name);

            $meta = [];
            $meta['uniqid']         = array_pop($meta_);
            $meta['created_at']     = array_pop($meta_);

            $meta['created_sec_ago']=time()-$meta['created_at'];

            array_shift($meta_);
            $meta['schema']         = implode('_',$meta_);

            $tables[$index] = [
                'fqd'       => $index,
                'keyspace'  => $table->keyspace_name,
                'name'      => $table->table_name,
            ];

            $tables[$index] = (object) array_merge($tables[$index],$meta);
        }

        return $tables;
    }

    public static function registerAutoShutdown()
    {
        declare(ticks = 1);

        self::$_shutdown = true;

        register_shutdown_function('\Simpletools\Db\Cassandra\TempTable::cleanup');

        pcntl_signal(SIGINT, '\Simpletools\Db\Cassandra\TempTable::cleanup');
        pcntl_signal(SIGQUIT, '\Simpletools\Db\Cassandra\TempTable::cleanup');
        pcntl_signal(SIGTERM, '\Simpletools\Db\Cassandra\TempTable::cleanup');
    }

    public static function registerMaxSize(int $size)
    {
        self::$_registerMaxSize = $size;
    }

    protected static function _register($fqd)
    {
        if(self::$_registerMaxSize && ($registeredTablesCount = count(self::$_registeredTables))>=self::$_registerMaxSize)
            throw new \Exception("You are trying to create: ".($registeredTablesCount+1)." temp tables; max: ".self::$_registerMaxSize,400);

        self::$_registeredTables[$fqd] = 1;
    }

    protected static function _unregister($fqd)
    {
        unset(self::$_registeredTables[$fqd]);
    }

    public static function cleanup($signo=0)
    {
        $client = new Client();

        foreach(self::$_registeredTables as $fqd => $int)
        {
            $client->execute('DROP TABLE IF EXISTS '.$fqd.';');
        }

        unset($client);

        if(self::$_shutdown) exit;
    }

    /*
     * THIS->
     */
    public function __construct($schema)
    {
        if(is_string($schema))
        {
            $this->schema = $schema;

            if(!isset(self::$_schemas[$schema]))
            {
                throw new Exception("Provided schema $schema has not been defined.",402);
            }
        }
        elseif(is_array($schema))
        {
            $this->schema           = 'noname';
            $this->thisTableName    = $this->_generateThisTempName();
            $this->_schema          = (new Schema($this->thisTableName))->describe($schema);
            $this->_schema->create();

            self::_register($this->_schema->keyspace() . '.' . $this->thisTableName);

            $this->_thisCreated     = true;
        }
    }

    public function name()
    {
        if(!$this->thisTableName)
            $this->thisTableName = $this->_generateThisTempName();

        return $this->thisTableName;
    }

    public function create()
    {
        if(!$this->_thisCreated)
        {
            if(!isset(self::$_schemas[$this->schema]))
                throw new Exception('Please define your schema first',400);

            $this->_schema = clone self::$_schemas[$this->schema];

            if (!$this->thisTableName)
                $this->thisTableName = $this->_generateThisTempName();

            if(!$this->_keep)
                self::_register($this->_schema->keyspace() . '.' . $this->thisTableName);

            $this->_schema->name($this->thisTableName)->create();

            $this->_thisCreated = true;
        }
        return $this;
    }

    protected function _generateThisTempName()
    {
        $name = 'tmp_'.$this->schema.'_'.time().'_'.uniqid();

        if($this->_appendix)
            $name .= '_'.$this->_appendix;

        return $name;
    }

    public function query()
    {
        if(!$this->_thisCreated)
            $this->create();

        return (new Query($this->thisTableName));
    }

    public function keep()
    {
        $this->_keep = true;

        return $this;
    }

    public function appendix($appendix)
    {
        if($this->_thisCreated)
            throw new Exception("Too late, table has been already created by now",400);

        $this->_appendix = $appendix;

        return $this;
    }

    public function noKeep()
    {
        $this->_keep = false;

        return $this;
    }

    public function drop()
    {
        if($this->_thisCreated)
        {
            $client = new Client();

            $client->execute('DROP TABLE IF EXISTS ' . $this->_schema->keyspace() . '."' . $this->_schema->name() . '";');

            if(!$this->_keep)
                self::_unregister($this->_schema->keyspace() . '.' . $this->_schema->name());

            $this->_thisCreated = false;
        }
    }

    public function __destruct()
    {
        if(!$this->_keep)
            $this->drop();
    }
}