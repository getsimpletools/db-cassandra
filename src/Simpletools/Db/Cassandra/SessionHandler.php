<?php

namespace Simpletools\Db\Cassandra;

use Simpletools\Db\Cassandra\Type\Uuid;

class SessionHandler implements \SessionHandlerInterface, \SessionIdInterface
{
    protected $_sessionTable;

    //time in sec, defaults to php ini session.gc_maxlifetime, however if that can't be read from php.ini, it will fail-over to 1800 sec
    protected $_maxLifeTime = 1800;

    protected static $_settings;

    public function __construct(array $settings = [])
    {
        if(isset($settings['maxLifeTime']))
        {
            $this->_maxLifeTime = $settings['maxLifeTime'];
        }
        elseif(isset(self::$_settings['maxLifeTime']) && self::$_settings['maxLifeTime'])
        {
            $this->_maxLifeTime = self::$_settings['maxLifeTime'];
        }
        elseif (get_cfg_var("session.gc_maxlifetime"))
        {
            $this->_maxLifeTime = get_cfg_var("session.gc_maxlifetime");
        }
    }

    public function maxLifeTime($seconds=null)
    {
        if(!$seconds)
            return $this->_maxLifeTime;
        else
            $this->_maxLifeTime = $seconds;

        return $this;
    }

    public static function settings($settings)
    {
        self::$_settings = [
            'cluster'       => @$settings['cluster'],
            'keyspace'      => @$settings['keyspace'],
            'table'         => @$settings['table'],
            'consistency'   => @$settings['consistency'],
            'maxLifeTime'   => @$settings['maxLifeTime']
        ];
    }

    public static function setup()
    {
        $client = new Client(self::$_settings['cluster']);

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].' (
              id uuid,
              data varchar,
              date_modified TIMESTAMP,
              date_expires TIMESTAMP,
              PRIMARY KEY (id)
            )'
        );
    }

    public function create_sid()
    {
        return (string) (new Uuid());
    }

    public function open($savePath, $sessionName)
    {
        return true;
    }

    public function close()
    {
        return true;
    }

    public function read($id)
    {
        try
        {
            $q = $this->getQuery();

            $res = $q->where('id',$id);

            if($res->isEmpty()) return "";
            else return $res->data;

            unset($q);

            return $res;
        }
        catch (\Exception $e)
        {
            throw $e;
        }
    }

    protected function getQuery()
    {
        $q = (new Query(self::$_settings['table'],self::$_settings['keyspace']))
            ->client(new Client(self::$_settings['cluster']));

        if(@self::$_settings['consistency'])
        {
            $q->options([
                'consistency'   => self::$_settings['consistency']
            ]);
        }

        return $q;
    }

    public function write($id, $data)
    {
        $q = $this->getQuery();

        $q->set([
            'id'                =>  $id,
            'data'              =>  $data,
            'date_modified'     =>  time(),
            'date_expires'      =>  time()+$this->_maxLifeTime
        ])->expires($this->_maxLifeTime)->run();

        unset($q);

        return true;
    }

    public function destroy($id)
    {
        $this->_sessionTable->delete([
            'id'    => $id
        ])->run();

        return true;
    }

    public function gc($maxlifetime)
    {
        return true;
    }
}
