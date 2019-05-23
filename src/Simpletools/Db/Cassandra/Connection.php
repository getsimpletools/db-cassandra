<?php

namespace Simpletools\Db\Cassandra;

class Connection
{
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