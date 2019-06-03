<?php

namespace Simpletools\Db\Cassandra\Type;

use Simpletools\Db\Cassandra\Client;
use Simpletools\Db\Cassandra\Exception;
use Simpletools\Db\Cassandra\Query;
use Simpletools\Db\Cassandra\Cql;

class AutoIncrement
{
    protected $_tag;

    protected static $_settings = array();

    protected $_client;
    protected $_counter;

    protected $_waitLockAcquisition = 1; //1 sec
    protected $_lockAcquisitionTries = 0;

    protected $_startTime;
    protected $_finishTime;

    protected $_cast;

    public static function settings($settings)
    {
        self::$_settings = [
            'cluster'   => @$settings['cluster'],
            'keyspace'  => @$settings['keyspace'],
            'table'     => @$settings['table']
        ];
    }

    public function locksTaken()
    {
        $this->id();

        return $this->_lockAcquisitionTries;
    }

    public function timeTaken()
    {
        $this->id();

        return $this->_finishTime-$this->_startTime;
    }

    public function await($time=null)
    {
        if($time===null) return $this->_waitLockAcquisition;

        $this->_waitLockAcquisition = $time;

        return $this;
    }

    public static function setup()
    {
        $client = new Client(self::$_settings['cluster']);

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].' (
              tag varchar,
              seq counter,
              PRIMARY KEY (tag)
            )'
        );

        $client->execute('
            CREATE TABLE IF NOT EXISTS '.self::$_settings['keyspace'].'.'.self::$_settings['table'].'_lock'.' (
              tag varchar,
              author uuid,
              PRIMARY KEY (tag)
            )
        ');
    }

    public function __construct($tag)
    {
        $this->_tag     = $tag;
        $this->_author  = new Uuid();
        $this->_client  = new Client(self::$_settings['cluster']);
    }

    protected function _checkLockTime()
    {
        if(microtime(true)-$this->_startTime>$this->_waitLockAcquisition)
        {
            throw new Exception("Exceeded lock timeout",500);
        }
    }

    public function cast($type)
    {
        $this->_cast = $type;

        return $this;
    }

    protected function _cast()
    {
        $val = $this->_counter->value();

        if($this->_cast && $this->_cast!='bigint')
        {
            settype($val, $this->_cast);
            return $val;
        }
        /*
        elseif(PHP_INT_MAX>2147483647)
        {
            return (int) $val;
        }
        */
        else
        {
            return new BigInt($this->_counter);
        }
    }

    public function id()
    {
        if(isset($this->_counter)) return $this->_cast();

        $this->_startTime = microtime(true);

        while(true)
        {
            (new Query(self::$_settings['table'] . '_lock'))
                    ->options([
                        'consistency'   => Query::CONSISTENCY_ALL
                    ])
                    ->insertIgnore($body = [
                        'tag' => $this->_tag,
                        'author' => $this->_author
                    ])
                    ->expires(ceil($this->_waitLockAcquisition))
                    ->run();

            /*
             * HAVE LOCK BEEN ACQUIRED?
             */
            $res = (new Query(self::$_settings['table'] . '_lock'))
                ->where('tag', $this->_tag);

            $this->_lockAcquisitionTries++;

            /*
             * YES
             */
            if(!$res->isEmpty() && ((string) $res->author == (string) $this->_author))
            {
                $this->_checkLockTime();

                $q = (new Query(self::$_settings['table']))
                    ->options([
                        'consistency'   => Query::CONSISTENCY_ALL
                    ])
                    ->update([
                        'seq'   => new Cql('seq + 1')
                    ])
                    ->where('tag',$this->_tag);

                $q->run();

                (new Query(self::$_settings['table'] . '_lock'))
                    ->options([
                        'consistency'   => Query::CONSISTENCY_ALL
                    ])
                    ->delete('tag',$this->_tag)
                    ->run();

                $seq = (new Query(self::$_settings['table']))
                    ->where('tag',$this->_tag);

                $this->_checkLockTime();

                /*
                 * POSSIBLE EXTRA CHECK BUT COUNTER WOULD HAVE TO BE REPLACED WITH BIGINT AS TYPE
                 */
                /*
                if((string) $seq->author!= (string) $this->_author)
                {
                    usleep(10);
                    continue;
                }
                */

                $this->_counter = $seq->seq;

                $this->_finishTime = microtime(true);

                return $this->_cast();
            }
            /*
             * NO
             */
            else
            {
                $this->_checkLockTime();
                usleep(10);
                continue;
            }
        }

        return $this->_tag;
    }

    public function __toString()
    {
        return (string) $this->id();
    }

    public function toInt()
    {
        return (int) (string) $this->id();
    }
}