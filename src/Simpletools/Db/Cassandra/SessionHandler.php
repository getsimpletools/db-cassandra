<?php

namespace Simpletools\Db\Cassandra;

use Simpletools\Db\Cassandra\Type\Uuid;

class SessionHandler implements \SessionHandlerInterface, \SessionIdInterface
{
	protected $_sessionTable;

	//time in sec, defaults to php ini session.gc_maxlifetime, however if that can't be read from php.ini, it will fail-over to 1800 sec
	protected static $_maxLifeTime = 1800;

	protected static $_settings;
	protected static $_logFile;

	protected static $_onWriteException;


	public function __construct(array $settings = [])
	{
		if(isset($settings['maxLifeTime']))
		{
			self::$_maxLifeTime = $settings['maxLifeTime'];
		}
		elseif(isset(self::$_settings['maxLifeTime']) && self::$_settings['maxLifeTime'])
		{
			self::$_maxLifeTime = self::$_settings['maxLifeTime'];
		}
		elseif (get_cfg_var("session.gc_maxlifetime"))
		{
			self::$_maxLifeTime = get_cfg_var("session.gc_maxlifetime");
		}



		self::$_maxLifeTime = (int) self::$_maxLifeTime;

		if(self::$_maxLifeTime<1)
			throw new Exception('maxLifeTime can\'t be smaller than 1 due to TimeWindowCompactionStrategy');
	}

	public function maxLifeTime($seconds=null)
	{
		if(!$seconds)
			return self::$_maxLifeTime;
		else
			self::$_maxLifeTime = $seconds;

		return $this;
	}

	public static function regenerateSessionId()
	{
		if(session_status()!=PHP_SESSION_ACTIVE)
		{
			session_start();
		}

		$currentId = session_id();


		$newId = (string) new Uuid();

		try {
			$currentSession = (new Doc($currentId))
				->client(new Client(self::$_settings['cluster']))
				->keyspace(self::$_settings['keyspace'])
				->table(self::$_settings['table'])
				->load();
		}
		catch(\Exception $e)
		{

			if(self::$_logFile)
				file_put_contents(self::$_logFile,
					date('Y-m-d H:i:s')."|REGENERATE|session_id:".$currentId."|EX:".$e->getMessage()."\n"
					, FILE_APPEND);

			return false;

		}

		$batch = (new Batch())
			->client(new Client(self::$_settings['cluster']));

		if(@self::$_settings['consistency'])
			$batch->consistency(self::$_settings['consistency']);

		if(@self::$_settings['writeConsistency'])
			$batch->consistency(self::$_settings['writeConsistency']);

		//session_unset();
		//session_destroy();
		session_write_close();
		//setcookie(session_name(),'',0,'/');

		$batch
			->add(
				(new Query(self::$_settings['table'],self::$_settings['keyspace']))->delete('id',$currentId)
			)
			->add(
				(new Query(self::$_settings['table'],self::$_settings['keyspace']))->set([
					'id'                =>  $newId,
					'data'              =>  (string) $currentSession->body->data
				])->expires(self::$_maxLifeTime))
			->run();

		session_id($newId);
		session_start();

		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|REGENERATE|curent_session_id:".$currentId."|new_session_id".$newId."|real_session_id:".session_id()."|data:".json_encode($currentSession->body->data)."\n"
				, FILE_APPEND);

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

		if(isset($settings['logFile']))
		{
			self::$_logFile = $settings['logFile'];
		}
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
		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|CREATE SID\n"
				, FILE_APPEND);

		return (string) (new Uuid());
	}

	public function open($savePath, $sessionName)
	{
		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|OPEN\n"
				, FILE_APPEND);

		return true;
	}

	public function close()
	{
		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|CLOSE\n"
				, FILE_APPEND);

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

			if(self::$_logFile)
				file_put_contents(self::$_logFile,
					date('Y-m-d H:i:s')."|READ|session_id:".$id."|RES:".(json_encode($res->data))."\n"
					, FILE_APPEND);

			if($res->isEmpty()) return "";
			else return $res->data;

			unset($q);

			return $res;
		}
		catch (\Exception $e)
		{
			if(self::$_logFile)
				file_put_contents(self::$_logFile,
					date('Y-m-d H:i:s')."|READ|session_id:".$id."|EX:".$e->getMessage()."\n"
					, FILE_APPEND);

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

		try {
            $q->set([
                'id' => $id,
                'data' => $data
            ])->expires(self::$_maxLifeTime)->run();

            if (self::$_logFile)
                file_put_contents(self::$_logFile,
                    date('Y-m-d H:i:s') . "|WRITE|session_id:" . $id . "|data:" . json_encode($data) . "|Expiry:" . self::$_maxLifeTime . "\n"
                    , FILE_APPEND);
        }
        catch(\Exception $e)
        {
            if(self::$_onWriteException)
            {
                $callback = self::$_onWriteException;
                $callback($e);
            }

            if (self::$_logFile)
                file_put_contents(self::$_logFile,
                    date('Y-m-d H:i:s')."|WRITE|session_id:".$id."|EX:".$e->getMessage()."\n"
                    , FILE_APPEND);
        }

        unset($q);

		return true;
	}

	public function destroy($id)
	{
		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|DESTROY|session_id:".$id."\n"
				, FILE_APPEND);

		$q = $this->getQuery();

		$q->delete('id', $id)->run();

		/*
		if (ini_get("session.use_cookies")) {
				$params = session_get_cookie_params();
				setcookie(session_name(), '', time() - 42000,
						$params["path"], $params["domain"],
						$params["secure"], $params["httponly"]
				);
		}
		*/

		unset($q);

		return true;
	}

	public function gc($maxlifetime)
	{
		if(self::$_logFile)
			file_put_contents(self::$_logFile,
				date('Y-m-d H:i:s')."|GC\n"
				, FILE_APPEND);

		return true;
	}

	public static function onWriteException(callable $callback)
    {
        self::$_onWriteException = $callback;
    }
}
