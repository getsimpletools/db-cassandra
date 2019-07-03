<?php

namespace Simpletools\Db\Cassandra;

class Connection
{
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

    const ROUTING_TOKEN_AWARE       = "ROUTING_TOKEN_AWARE";
    const ROUTING_LATENCY_AWARE     = "ROUTING_LATENCY_AWARE";

    protected static $_connectors = array(

    );

    public static function getOne($name)
    {
        return isset(self::$_connectors[$name]) ? self::$_connectors[$name] : null;
    }

    public static function setOne($name,$session)
    {
        self::$_connectors[$name] = $session;
    }
}