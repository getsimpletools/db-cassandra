<?php

namespace Simpletools\Db\Cassandra;

class Client
{
		protected static $_gSettings            = array();
		protected static $_pluginSettings            = array(); //['convertMapToJson' => true|false]

		protected static $_defaultCluster       = 'default';
    protected $___cluster	                = 'default';

    protected $___keyspace;
    protected $___connection;


    public function __construct($cluster=null)
    {
        if($cluster)
            $this->___cluster = $cluster;
        else
            $this->___cluster = self::$_defaultCluster;

        if(isset(self::$_gSettings[$cluster]['keyspace']))
        {
            $this->___keyspace = self::$_gSettings[$cluster]['keyspace'];
        }
        elseif(isset(self::$_gSettings[self::$_defaultCluster]['keyspace']))
        {
            $this->___keyspace = self::$_gSettings[self::$_defaultCluster]['keyspace'];
        }

        if($cluster && !isset(self::$_gSettings[$cluster]))
        {
            throw new Exception("No settings for provided cluser $cluster",400);
        }

				self::$_pluginSettings[$cluster] = array();
    }

    public static function cluster(array $settings,$cluster='default')
    {
        $cluster                = (isset($settings['name']) ? $settings['name'] : $cluster);
        $default                = (isset($settings['default']) ? (bool) $settings['default'] : false);

        $settings['host']       = isset($settings['host']) ? $settings['host'] : @$settings['hosts'];
        $settings['port']       = isset($settings['port']) ? (int) $settings['port'] : 9042;

        $settings['keyspace']   = isset($settings['keyspace']) ? $settings['keyspace'] : null;

        if(!isset($settings['host']))
        {
            throw new \Exception('Please specify host or hosts');
        }

        if(!is_array($settings['host']))
        {
            $settings['host'] = array($settings['host']);
        }

        if($default)
        {
            self::$_defaultCluster = $cluster;
        }

        self::$_gSettings[$cluster] = $settings;
    }

    public function getCluster()
		{
			return $this->___cluster;
		}

		public function getClusterSettings()
		{
			return self::$_gSettings[$this->___cluster];
		}

    public static function setPluginSetting($settingName, $value, $cluster='default')
		{
			self::$_pluginSettings[$cluster][$settingName] = $value;
		}

		public static function getPluginSetting($settingName,$cluster='default')
		{
			return @self::$_pluginSettings[$cluster][$settingName];
		}

    public function keyspace($keyspace=null)
    {
        if($keyspace)
        {
            $this->___keyspace = $keyspace;
            return $this;
        }
        else
        {
            return $this->___keyspace;
        }
    }

    public function connector()
    {
        $this->connect();

        return $this->___connection;
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
						->withRetryPolicy(new \Cassandra\RetryPolicy\DefaultPolicy())
            ->withRoundRobinLoadBalancingPolicy(); //todo - enable more LB policies

        if(isset($settings['consistency']))
            $cluster->withDefaultConsistency($settings['consistency']);

        if(isset($settings['pageSize']))
            $cluster->withDefaultPageSize($settings['pageSize']);

        if(isset($settings['timeout']))
            $cluster->withDefaultTimeout($settings['timeout']);

				if(isset($settings['connectTimeout']))
					$cluster->withConnectTimeout($settings['connectTimeout']);

				if(isset($settings['requestTimeout']))
					$cluster->withRequestTimeout($settings['requestTimeout']);

        if(@$settings['username'] && @$settings['password'])
            $cluster->withCredentials($settings['username'], $settings['password']);

				if(isset($settings['ioThreads']))
					$cluster->withIOThreads($settings['ioThreads']);

				if(isset($settings['retryPolicy']))
				{
					if($settings['retryPolicy'] == 'DefaultPolicy')
						$cluster->withRetryPolicy(new \Cassandra\RetryPolicy\DefaultPolicy());
					elseif($settings['retryPolicy'] == 'DowngradingConsistency')
						$cluster->withRetryPolicy(new \Cassandra\RetryPolicy\DowngradingConsistency());
					elseif($settings['retryPolicy'] == 'Fallthrough')
						$cluster->withRetryPolicy(new \Cassandra\RetryPolicy\Fallthrough());
					elseif($settings['retryPolicy'] == 'Logging' && isset($settings['retryPolicyLogging']))
						$cluster->withRetryPolicy(new \Cassandra\RetryPolicy\Logging($settings['retryPolicyLogging']));
				}

        if(isset($settings['routing']))
        {
            if(is_array($settings['routing']))
            {
                foreach($settings['routing'] as $routing)
                {
                    if ($routing == Connection::ROUTING_TOKEN_AWARE) {
                        $cluster->withTokenAwareRouting(true);
                        break;
                    }

                    if ($routing == Connection::ROUTING_LATENCY_AWARE) {
                        $cluster->withLatencyAwareRouting(true);
                        break;
                    }
                }
            }
            else
            {
                if ($settings['routing'] == Connection::ROUTING_TOKEN_AWARE)
                    $cluster->withTokenAwareRouting(true);

                if ($settings['routing'] == Connection::ROUTING_LATENCY_AWARE)
                    $cluster->withLatencyAwareRouting(true);
            }
        }


        if(@$settings['persistentSessions'])
            $cluster->withPersistentSessions((bool) $settings['persistentSessions']);


        $session = $cluster->build()->connect();
        $this->___connection = $session;

        Connection::setOne($this->___cluster,$this->___connection);

        return $this;
    }

    protected $_preparedQuery;

    public function prepare($preparedQuery)
    {
        $this->connect();

        $this->_preparedQuery           = $this->___connection->prepare($preparedQuery);

        return $this;
    }

    protected $_queryOptions = array();

    public function queryOptions($options)
    {
        $this->_queryOptions    = $options;

        return $this;
    }

    public function execute($input=null,$thisQueryOptions=null)
    {
        $this->connect();

        $queryOptions   = $this->_queryOptions;

        if($thisQueryOptions && is_array($thisQueryOptions))
            $queryOptions = array_merge($queryOptions,$thisQueryOptions);

        $query          = $this->_preparedQuery;

        if($input && is_array($input))
        {
            $compiledArguments = [];
            foreach ($input as $arg) {
                if ($arg instanceof Type\Uuid)
                    $compiledArguments[] = new \Cassandra\Uuid((string) $arg);

                elseif (
                    $arg instanceof Type\Timestamp OR
                    $arg instanceof Type\BigInt OR
										$arg instanceof Type\Map
                ) {
                    $compiledArguments[] = $arg->value();
                }

                elseif ($arg instanceof Type\AutoIncrement)
                    $compiledArguments[] = new \Cassandra\BigInt((string) $arg->value());

                else
                    $compiledArguments[] = $arg;
            }

            $queryOptions = array_merge($this->_queryOptions, array(
                'arguments' => $compiledArguments
            ));
        }
        elseif($input && is_string($input))
        {
            $query = $input;
        }

        $result = $this->___connection->execute($query, $queryOptions);

        return new Result($result,$this->___connection);
    }

    public function escape($string,$fromEncoding='UTF-8',$toEncoding='UTF-8')
    {
        if($fromEncoding!='UTF-8' OR $toEncoding!='UTF-8')
        {
            $string = mb_convert_encoding ($string,$fromEncoding,$toEncoding);
        }

        $search = array("\\",  "\x00", "\n",  "\r",  "'",  '"', "\x1a");
        $replace = array("\\\\","\\0","\\n", "\\r", "\'", '\"', "\\Z");

        return str_replace($search, $replace, $string);
    }

    public function __get($table)
    {
        $query = new Query($table, $this->___keyspace);

        return $query;
    }

    public function __call($table,$args)
    {
				if(count($args) == 1)
				{
					$args = $args[0];
				}

        $query = new Query($table, $this->___keyspace);
        $query->columns($args);

        return $query;
    }
}
