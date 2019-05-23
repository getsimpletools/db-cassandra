<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Client;

class AutoInc
{
    protected $_id;

    protected static $_settings = array();

    protected $_client;
    protected $_counter;

    public static function settings($settings)
    {

        self::$_settings = [
            'cluster'   => @$settings['cluster'],
            'keyspace'  => @$settings['keyspace'],
            'table'     => @$settings['table']
        ];
    }

    public static function setup()
    {
        $client = new Client(self::$_settings['cluster']);

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].' (
              id varchar,
              seq counter,
              PRIMARY KEY (id)
            )'
        );

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].'_lock'.' (
              id varchar,
              author uuid,
              PRIMARY KEY (id)
            )'
        );
    }

    public function __construct($id)
    {
        $this->_id = $id;
        $this->_client = new Client(self::$_settings['cluster']);
    }

    public function id()
    {
        $res = $this->_client->{self::$_settings['table']. '_lock'}->where('id',$this->_id);

        if($res->isEmpty())
        {
            $author = new Uuid();
            $timeoutSec    = 3;
            $start      = microtime(true);
            $duration   = 0;

            while(true)
            {
                echo 'sss';
                echo $this->_client->{self::$_settings['table'] . '_lock'}->insertIgnore($body = [
                    'id' => $this->_id,
                    'author' => $author
                ])
                    ->expires(20)->run();

                print_r($body);

                echo 'done';
                exit;

                $res = $this->_client->{self::$_settings['table'] . '_lock'}->where('id', $this->_id);
                if ($res->isEmpty() OR $res->author != $author) {

                    $duration = microtime(true)-$start;
                    if($duration>$timeoutSec*1000000)
                    {
                        break;
                    }

                    usleep(rand(0,10000));
                    continue;
                }
                else
                {
                    break;
                }
            }
        }

        return $this->_id;
    }

    public function __toString()
    {
        return $this->id();
    }
}