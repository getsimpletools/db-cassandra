<?php

namespace Simpletools\Db\Cassandra;

class Client
{
    protected static $_gSettings    = array();
    protected $___cluster	        = 'default';

    protected $___connection;

    public static function settings(array $settings,$cluster='default')
    {
        $cluster            = (isset($settings['cluster']) ? $settings['cluster'] : $cluster);
        $settings['host']   = isset($settings['host']) ? $settings['host'] : @$settings['hosts'];
        $settings['port']   = isset($settings['port']) ? (int) $settings['host'] : 9042;

        if(!isset($settings['host']))
        {
            throw new \Exception('Please specify host or hosts');
        }

        if(!is_array($settings['host']))
        {
            $settings['host'] = array($settings['host']);
        }

        self::$_gSettings[$cluster] = $settings;
    }

    public function connect()
    {
        if(!isset(self::$_gSettings[$this->___cluster]))
        {
            throw new \Exception("Please specify your cluster settings first");
        }

        $this->___connection        = Connection::getOne($this->___cluster);
        if($this->___connection)
        {
            return $this;
        }

        $settings                   = self::$_gSettings[$this->___cluster];

        $cluster = \Cassandra::cluster();

        call_user_func_array(array($cluster,'withContactPoints'),$settings['host']);

        $cluster
            ->withPort($settings['port'])
            ->withRoundRobinLoadBalancingPolicy(); //todo - enable more LB policies

        if(@$settings['username'] && @$settings['password'])
            $cluster->withCredentials($settings['username'], $settings['password']);

        $this->___connection = $cluster->connect();

        Connection::setOne($this->___cluster,$this->___connection);

        return $this;
    }
}