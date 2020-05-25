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

        $this->_maxLifeTime = (int) $this->_maxLifeTime;

        if($this->_maxLifeTime<1)
            throw new Exception('maxLifeTime can\'t be smaller than 1 due to TimeWindowCompactionStrategy');
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
            'cluster'           => @$settings['cluster'],
            'keyspace'          => @$settings['keyspace'],
            'table'             => @$settings['table'],
            'consistency'       => @$settings['consistency'],
            'maxLifeTime'       => @$settings['maxLifeTime'],

            'readConsistency'   => @$settings['readConsistency'],
            'writeConsistency'  => @$settings['writeConsistency'],
        ];
    }

    public static function setup($compactionWindowSize=12,$compactionWindowUnit="HOURS")
    {
        $client = new Client(self::$_settings['cluster']);

        $default_time_to_live = strtotime('NOW + '.$compactionWindowSize.' '.$compactionWindowUnit)-time();

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].' (
              id text,
              data varchar,
              PRIMARY KEY (id)
            ) 
            WITH "compaction" = {\'class\':\'TimeWindowCompactionStrategy\',\'compaction_window_size\':\''.$compactionWindowSize.'\',\'compaction_window_unit\':\''.$compactionWindowUnit.'\'}
            AND default_time_to_live = '.$default_time_to_live.';'
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

            if(self::$_settings['readConsistency']!==null)
                $q->consistency(self::$_settings['readConsistency']);

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

        if(self::$_settings['writeConsistency']!==null)
            $q->consistency(self::$_settings['writeConsistency']);

        $q->set([
            'id'                =>  $id,
            'data'              =>  $data
        ])->expires($this->_maxLifeTime)->run();

        unset($q);

        return true;
    }

    public function destroy($id)
    {
        $q = $this->getQuery();

        $q->delete('id', $id)->run();

        if (ini_get("session.use_cookies")) {
            $params = session_get_cookie_params();
            setcookie(session_name(), '', time() - 42000,
                $params["path"], $params["domain"],
                $params["secure"], $params["httponly"]
            );
        }

        unset($q);

        return true;
    }

    public function gc($maxlifetime)
    {
        return true;
    }
}
