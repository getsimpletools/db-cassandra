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
 *
 */

namespace Simpletools\Db\Cassandra;

use \Simpletools\Db\Cassandra\Doc\Body;
use Simpletools\Db\Cassandra\Type\Uuid;
use Simpletools\Db\Cassandra\Type\BigInt;
use Simpletools\Db\Cassandra\Type\Map;
use Simpletools\Db\Cassandra\Type\Date;
use Simpletools\Db\Cassandra\Type\Timestamp;

class Docs implements \Iterator
{
	const LOGGED            = 0;
	const UNLOGGED          = 1;
	const BATCH_COUNTER     = 2;

	protected $_docs = [];
	protected $_query;
	protected $_currentIndex = 0;
	protected $_whereArgs;

	public function __construct(mixed $QueryOrArrayOfDoc = null)
	{
		if(is_array($QueryOrArrayOfDoc))
		{
			$this->_docs = $QueryOrArrayOfDoc;
		}
		elseif ($QueryOrArrayOfDoc instanceof Query)
		{
			$this->_query = $QueryOrArrayOfDoc;
			$this->_whereArgs = $this->_query->getWhereArguments();
		}
	}

	public function addDoc(Doc $doc)
	{
		$this->_docs[] = $doc;
	}

	public function save($type = self::LOGGED)
	{
		$batch = new Batch($type);

		foreach ($this->_docs as $doc)
		{
			$batch->add($doc->getSaveQuery());
			$doc->resetQuery();
		}

		$batch->run();
	}

	public function load()
	{
		if ($this->_query)
		{
			$this->_docs = [];
			$primaryKeys = $this->_query->getPrimaryKey();
			$result = $this->_query->fetchAll();
			$keyspace = $this->_query->getKeyspace();
			$table = $this->_query->getTable();

			foreach ($result as $res)
			{
				$ids = [];
				foreach ($primaryKeys as $pKey)
					$ids[$pKey] = $res->{$pKey};

				$this->_docs[] = (new Doc($ids))
						->keyspace($keyspace)
						->table($table)
						->body($res);
			}

			unset($result);
		}
		elseif ($this->_docs)
		{
			foreach ($this->_docs as $doc)
			{
				$doc->load();
			}
		}
	}

	public function length()
	{
		return count($this->_docs);
	}

	public function remove()
	{
		if ($this->_query)
		{
			$keys = [];
			foreach ($this->_whereArgs as $arg)
				$keys[$arg[0]] = $arg[1];

			$this->_query->delete(key($keys), array_shift($keys));

			foreach ($keys as $key =>$val)
				$this->_query->also($key, $val);

			$this->_query->resetResult()->run();
			$this->_docs = [];
		}
		elseif ($this->_docs)
		{
			foreach ($this->_docs as $doc)
			{
				$doc->remove();
			}
			$this->_docs = [];
		}
	}

	public function clean()
	{
		$this->_docs = [];
	}

	function rewind()
	{
		$this->_currentIndex = 0;
	}

	function current()
	{
		return $this->_docs[$this->_currentIndex];
	}

	function key()
	{
		return $this->_currentIndex;
	}

	function next()
	{
		$this->_currentIndex++;
		return $this->_docs[$this->_currentIndex] ?? null;
	}

	function valid()
	{
		return (!isset($this->_docs[$this->_currentIndex])) ? false : true;
	}

}
