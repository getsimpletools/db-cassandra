<?php

namespace Simpletools\Db\Cassandra;

class TempTable
{
    protected static $_schemas  = [];
    protected $schema           = '';
    protected $thisTableName    = '';
    protected $_appendix        = '';
    protected $_thisCreated     = false;
    protected $_keep            = false;

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
        return (new Query($this->thisTableName));
    }

    public function __get($table)
    {
        $query = new Query($table, $this->_schema->keyspace());

        return $query;
    }

    public function __call($table,$args)
    {
        if(count($args) == 1)
        {
            $args = $args[0];
        }

        $query = new Query($table, $this->_schema->keyspace());
        $query->columns($args);

        return $query;
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

    public function __destruct()
    {
        if($this->_thisCreated && !$this->_keep)
        {
            $client = new Client();
            $client->execute('DROP TABLE ' . $this->_schema->keyspace() . '."' . $this->_schema->name() . '";');
        }
    }
}