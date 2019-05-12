<?php

namespace Simpletools\Db\Cassandra\Id;

class RandomBytes
{
    protected $_id;
    protected static $_length = 20;
    protected $_olength;

    public function __construct()
    {
        $this->_olength = self::$_length;
    }

    public function entropy($entropy=null)
    {
        if(!$entropy) return $this->_olength;
        else $this->_olength = (int) $entropy;

        return $this;
    }

    public static function __callStatic($name, $arguments)
    {
        if ($name == 'entropy')
        {
            if($arguments)
                self::$_length = (int) $arguments[0];
            else
                return self::$_length;
        }
    }

    protected function _generate()
    {
        $length = $this->_olength;

        if(!$this->_id) {
            if (function_exists('random_bytes')) {
                $this->_id = bin2hex(random_bytes($length));
            } elseif (function_exists('openssl_random_pseudo_bytes')) {
                $this->_id = bin2hex(openssl_random_pseudo_bytes($length));
            } else {
                throw new Exception("No random bytes generator found, please install random_bytes() or openssl_random_pseudo_bytes()", 404);
            }
        }

        return $this->_id;
    }

    public function id()
    {
        return $this->_generate();
    }

    public function __toString()
    {
        return $this->id();
    }
}
