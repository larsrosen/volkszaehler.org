<?php
/**
 * Data aggregation utility
 *
 * @author Andreas Goetz <cpuidle@gmx.de>
 * @copyright Copyright (c) 2013, The volkszaehler.org project
 * @package util
 * @license http://opensource.org/licenses/gpl-license.php GNU Public License
 */
/*
 * This file is part of volkzaehler.org
 *
 * volkzaehler.org is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * any later version.
 *
 * volkzaehler.org is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with volkszaehler.org. If not, see <http://www.gnu.org/licenses/>.
 */

namespace Volkszaehler\Util;

use Volkszaehler\Util;
use Volkszaehler\Interpreter;
use Volkszaehler\Definition;
use Doctrine\DBAL;

class Aggregation {
	/**
	 * @var \Doctrine\DBAL\Connection Database connection
	 */
	protected $conn;

	/**
	 * @var SQL aggregation types and assorted date formats
	 */
	protected static $aggregationLevels = array();

	/**
	 * Initialize static variables
	 *
	 * @todo When changing order or this array the aggregation table must be rebuilt
	 */
	static function init() {
		self::$aggregationLevels = array(
			'second' => '"%Y-%m-%d %H:%i:%s"',	// type 0
			'minute' => '"%Y-%m-%d %H:%i:00"',	// type 1
			'hour' => 	'"%Y-%m-%d %H:00:00"',	// type 2
			'day' => 	'"%Y-%m-%d"',			// type 3
			'week' => 	null,					// type 4 - not supported
			'month' => 	'"%Y-%m-1"',			// type 5
			'year' => 	'"%Y-1-1"'				// type 6
		);
	}

	public function __construct(DBAL\Connection $conn) {
		$this->conn = $conn;
	}

	/**
	 * Get list of aggregation levels
	 *
	 * @param  string  $level aggregation level (e.g. 'day')
	 * @return boolean        validity
	 */
	public static function getAggregationLevels() {
		return array_keys(self::$aggregationLevels);
	}

	/**
	 * Test if aggregation level is valid and implemented
	 *
	 * @param  string  $level aggregation level (e.g. 'day')
	 * @return boolean        validity
	 */
	public static function isValidAggregationLevel($level) {
		return in_array($level, self::getAggregationLevels())
			&& (isset(self::$aggregationLevels[$level]));
	}

	/**
	 * Convert aggregation level to numeric type
	 *
	 * @param  string $level aggregation level (e.g. 'day')
	 * @return integer       aggregation level numeric value
	 */
	public static function getAggregationLevelTypeValue($level) {
		$type = array_search($level, self::getAggregationLevels(), true);
		return($type);
	}

	/**
	 * SQL format for grouping data by aggregation level
	 *
	 * @param  string $level aggregation level (e.g. 'day')
	 * @return string        SQL date format
	 */
	public static function getAggregationDateFormat($level) {
		return self::$aggregationLevels[$level];
	}

	/**
	 * Simple optimizer - choose aggregation level with most data available
	 *
	 * @param  string  $targetLevel desired highest level (e.g. 'day')
	 * @return boolean list of valid aggregation levels
	 */
	public function getOptimalAggregationLevel($uuid, $targetLevel = null) {
		$levels = self::getAggregationLevels();

		$sqlParameters = array($uuid);
		$sql = 'SELECT aggregate.type, COUNT(aggregate.id) AS count ' .
			   'FROM aggregate INNER JOIN entities ON aggregate.channel_id = entities.id ' .
			   'WHERE uuid = ? ';
		if ($targetLevel) {
			$sqlParameters[] = self::getAggregationLevelTypeValue($targetLevel);
			$sql .= 'AND aggregate.type <= ? ';
		}
		else {
			$sql .= 'AND count > 0 ';
		}
		$sql.= 'GROUP BY type ' .
			   'ORDER BY type DESC';

		$rows = $this->conn->fetchAll($sql, $sqlParameters);

		// append readable level name
		for ($i=0; $i<count($rows); $i++) {
			$rows[$i]['level'] = $levels[$rows[$i]['type']];
		}

		return count($rows) ? $rows : FALSE;
	}

	/**
	 * Remove aggregration data - either all or selected type
	 *
	 * @param  string $level aggregation level to remove data for
	 * @return int 			 number of affected rows
	 */
	public function clear($uuid = null, $level = 'all') {
		$sqlParameters = array();

		if ($level == 'all') {
			if ($uuid) {
				$sql = 'DELETE aggregate FROM aggregate ' .
					   'INNER JOIN entities ON aggregate.channel_id = entities.id ' .
					   'WHERE entities.uuid = ?';
				$sqlParameters[] = $uuid;
			}
			else {
				$sql = 'TRUNCATE TABLE aggregate';
			}
		}
		else {
			$sqlParameters[] = self::getAggregationLevelTypeValue($level);
			$sql = 'DELETE aggregate FROM aggregate ' .
				   'INNER JOIN entities ON aggregate.channel_id = entities.id ' .
				   'WHERE aggregate.type = ? ';
			if ($uuid) {
				$sql .= 'AND entities.uuid = ?';
				$sqlParameters[] = $uuid;
			}
		}

		if (Util\Debug::isActivated())
			echo(Util\Debug::getParametrizedQuery($sql, $sqlParameters)."\n");

		$rows = $this->conn->executeQuery($sql, $sqlParameters);
	}

	/**
	 * Core data aggregation
	 *
	 * @param  int 	  $channel_id  id of channel to perform aggregation on
	 * @param  string $interpreter interpreter class name
	 * @param  string $mode        aggregation mode (full, delta)
	 * @param  string $level       aggregation level (day...)
	 * @param  int 	  $period      delta days to aggregate
	 * @return int    number of rows
	 */
	protected function aggregateChannel($channel_id, $interpreter, $mode, $level, $period) {
		$format = self::getAggregationDateFormat($level);
		$type = self::getAggregationLevelTypeValue($level);

		// get interpreter's aggregation function
		$aggregationFunction = call_user_func(array($interpreter, 'groupExprSQL'), 'value');

		$sqlParameters = array($type);
		$sql = 'REPLACE INTO aggregate (channel_id, type, timestamp, value, count) ' .
			   'SELECT channel_id, ? AS type, MAX(timestamp) AS timestamp, ' .
			   $aggregationFunction . ' AS value, COUNT(timestamp) AS count ' .
			   'FROM data WHERE ';

		// selected channel only
		if ($channel_id) {
			$sqlParameters[] = $channel_id;
			$sql .= 'channel_id = ? ';
		}

		// since last aggregation only
		if ($mode == 'delta') {
			if ($channel_id) {
				// selected channel
				$sqlTimestamp = 'SELECT UNIX_TIMESTAMP(DATE_ADD(' .
						   	   		   'FROM_UNIXTIME(MAX(timestamp) / 1000, ' . $format . '), ' .
						   	   		   'INTERVAL 1 ' . $level . ')) * 1000 ' .
							   	'FROM aggregate ' .
							   	'WHERE type = ? AND channel_id = ?';
				if ($ts = $this->conn->fetchColumn($sqlTimestamp, array($type, $channel_id), 0)) {
					$sqlParameters[] = $ts;
					$sql .= 'AND timestamp >= ? ';
				}
			}
			else {
				// all channels
				$sqlParameters[] = $type;
				$sql .=
				   'AND timestamp >= IFNULL((' .
				   	   'SELECT UNIX_TIMESTAMP(DATE_ADD(' .
				   	   		  'FROM_UNIXTIME(MAX(timestamp) / 1000, ' . $format . '), ' .
				   	   		  'INTERVAL 1 ' . $level . ')) * 1000 ' .
					   'FROM aggregate ' .
					   'WHERE type = ? AND aggregate.channel_id = data.channel_id ' .
				   '), 0) ';
			}
		}

		// selected number of periods only
		if ($period) {
			$sql .=
			   'AND timestamp >= (SELECT UNIX_TIMESTAMP(DATE_SUB(DATE_FORMAT(NOW(), ' . $format . '), INTERVAL ? ' . $level . ')) * 1000) ';
			$sqlParameters[] = $period;
		}

		// up to before current period
		$sql.= 'AND timestamp < UNIX_TIMESTAMP(DATE_FORMAT(NOW(), ' . $format . ')) * 1000 ' .
			   'GROUP BY channel_id, ' . Interpreter\Interpreter::buildGroupBySQL($level);

		if (Util\Debug::isActivated())
			echo(Util\Debug::getParametrizedQuery($sql, $sqlParameters)."\n");

		$rows = $this->conn->executeUpdate($sql, $sqlParameters);

		return($rows);
	}

	/**
	 * Core data aggregation wrapper
	 *
	 * @param  string $uuid   channel UUID
	 * @param  string $level  aggregation level (e.g. 'day')
	 * @param  string $mode   'full' or 'delta' aggretation
	 * @param  int    $period number of prior periods to aggregate in delta mode
	 * @return int         	  number of affected rows
	 */
	public function aggregate($uuid = null, $level = 'day', $mode = 'full', $period = null) {
		// validate settings
		if (!in_array($mode, array('full', 'delta'))) {
			throw new \Exception('Unsupported aggregation mode ' . $mode);
		}
		if (!$this->isValidAggregationLevel($level)) {
			throw new \Exception('Unsupported aggregation level ' . $level);
		}

		// get channel definition to select correct aggregation function
		$sqlParameters = array('channel');
		$sql = 'SELECT id, uuid, type FROM entities WHERE class = ?';
		if ($uuid) {
			$sqlParameters[] = $uuid;
			$sql .= ' AND uuid = ?';
		}

		$rows = 0;

		// aggregate each channel
		foreach ($this->conn->fetchAll($sql, $sqlParameters) as $row) {
			$entity = Definition\EntityDefinition::get($row['type']);
			$interpreter = $entity->getInterpreter();

			$rows += $this->aggregateChannel($row['id'], $interpreter, $mode, $level, $period);
		}

		return($rows);
	}
}

// initialize static variables
Aggregation::init();

?>
