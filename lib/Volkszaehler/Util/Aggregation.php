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
use Doctrine\DBAL;

class Aggregation {
	/**
	 * @var \Doctrine\DBAL\Connection Database connection
	 */
	protected $conn;

	/**
	 * @var SQL aggregation types and assorted date formats
	 */
	protected static $aggregation_types = array();

	/**
	 * Initialize static variables
	 * @todo When changing order or this array the aggregation table must be rebuilt
	 */
	static function init() {
		self::$aggregation_types = array(
			'second' => '"%Y-%m-%d %H:%i:%s"',	// type 0
			'minute' => '"%Y-%m-%d %H:%i:00"',	// type 1
			'hour' => 	'"%Y-%m-%d %H:00:00"',	// type 2
			'day' => 	'"%Y-%m-%d"',			// type 3
			'week' => 	null,					// type 4 - not supported
			'month' => 	'"%Y-%m-1"',			// type 5
			'year' => 	'"%Y-1-1"'				// type 6
		);
	}

	public function __construct($conn) {
		$this->conn = $conn; // DBAL connection
	}

	/**
	 * Remove aggregration data - either all or selected type
	 * @param  string $level aggregation level to remove data for
	 * @return int 			 number of affected rows
	 */
	public function clear($level = 'all') {
		$sqlParameters = array();

		if ($level == 'all') {
			$sql = 'TRUNCATE TABLE aggregate';
		}
		else {
			$sqlParameters[] = $level;
			$sql = 'DELETE FROM aggregate WHERE type=?';
		}

		if (Util\Debug::isActivated())
			echo(Util\Debug::getParametrizedQuery($sql, $sqlParameters)."\n");

		$rows = $this->conn->exec($sql, $sqlParameters);
	}

	/**
	 * Test if aggregation level is valid and implemented
	 * @param  string  $level aggregation level (e.g. 'day')
	 * @return boolean        validity
	 */
	public static function isValidAggregationLevel($level) {
		return in_array($level, array_keys(self::$aggregation_types))
			&& (isset(self::$aggregation_types[$level]));
	}

	/**
	 * Convert aggregation level to numeric type
	 * @param  string $level aggregation level (e.g. 'day')
	 * @return integer       aggregation level numeric value
	 */
	public static function getAggregationLevelTypeValue($level) {
		$levels = array_keys(self::$aggregation_types);
		$type = array_search($level, $levels, true);

		return($type);
	}

	/**
	 * SQL format for grouping data by aggregation level
	 * @param  string $level aggregation level (e.g. 'day')
	 * @return string        SQL date format
	 */
	public static function getAggregationDateFormat($level) {
		return self::$aggregation_types[$level];
	}

	/**
	 * Core data aggregration
	 * @param  string $mode   'full' or 'delta' aggretation
	 * @param  string $level  aggregation level (e.g. 'day')
	 * @param  int    $period number of prior periods to aggregate in delta mode
	 * @return int         	  number of affected rows
	 */
	public function aggregate($mode = 'full', $level = 'day', $period = null) {
		// validate settings
		if (!in_array($mode, array('full', 'delta'))) {
			throw new \Exception('Unsupported aggregation mode ' . $mode);
		}
		if (!$this->isValidAggregationLevel($level)) {
			throw new \Exception('Unsupported aggregation level ' . $level);
		}

		$format = self::getAggregationDateFormat($level);

		$sqlParameters = array(self::getAggregationLevelTypeValue($level));
		$sql = 'REPLACE INTO aggregate (channel_id, type, timestamp, value, count) ' .
			   'SELECT channel_id, ? AS type, MAX(timestamp) AS timestamp, SUM(value) AS value, COUNT(timestamp) AS count ' .
			   'FROM data ' .
			   'WHERE timestamp < UNIX_TIMESTAMP(DATE_FORMAT(NOW(), ' . $format . ')) * 1000 ';
		if ($mode == 'delta') {
			$sql .=
			   'AND timestamp >= IFNULL((' .
		   	   'SELECT UNIX_TIMESTAMP(DATE_ADD(' .
		   	   		'FROM_UNIXTIME(MAX(timestamp) / 1000, ' . $format . '), ' .
		   	   		'INTERVAL 1 ' . $level . ')) * 1000 ' .
			   'FROM aggregate ' .
			   'WHERE channel_id = data.channel_id AND type = ?), 0) ';
			$sqlParameters[] = self::getAggregationLevelTypeValue($level);
		}
		if ($period) {
			$sql .=
			   'AND timestamp >= (SELECT UNIX_TIMESTAMP(DATE_SUB(DATE_FORMAT(NOW(), ' . $format . '), INTERVAL ? ' . $level . ')) * 1000) ';
			$sqlParameters[] = $period;
		}
		$sql.= 'GROUP BY channel_id, ' . Interpreter\Interpreter::buildGroupBySQL($level);

		if (Util\Debug::isActivated())
			echo(Util\Debug::getParametrizedQuery($sql, $sqlParameters)."\n");

		$rows = $this->conn->executeUpdate($sql, $sqlParameters);
		return($rows);
	}
}

// initialize static variables
Aggregation::init();

?>
