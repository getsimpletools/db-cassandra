<?php
/*
 * Simpletools Framework.
 * Copyright (c) 2009, Marcin Rosinski. (https://www.getsimpletools.com)
 * All rights reserved.
 *
 * LICENCE
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - 	Redistributions of source code must retain the above copyright notice,
 * 		this list of conditions and the following disclaimer.
 *
 * -	Redistributions in binary form must reproduce the above copyright notice,
 * 		this list of conditions and the following disclaimer in the documentation and/or other
 * 		materials provided with the distribution.
 *
 * -	Neither the name of the Simpletools nor the names of its contributors may be used to
 * 		endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @framework		Simpletools
 * @copyright  		Copyright (c) 2009 Marcin Rosinski. (http://www.getsimpletools.com)
 * @license    		http://www.opensource.org/licenses/bsd-license.php - BSD
 * @depends         https://datastax.github.io/php-driver/features/
 *
 */

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