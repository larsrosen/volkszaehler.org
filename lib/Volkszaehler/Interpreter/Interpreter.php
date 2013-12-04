<?php
/**
 * @copyright Copyright (c) 2011, The volkszaehler.org project
 * @package default
 * @license http://www.opensource.org/licenses/gpl-license.php GNU Public License
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

namespace Volkszaehler\Interpreter;

use Volkszaehler\Util;
use Volkszaehler\Model;
use Doctrine\ORM;

/**
 * Interpreter superclass for all interpreters
 *
 * @author Steffen Vogel <info@steffenvogel.de>
 * @package default
 */
abstract class Interpreter {
	protected $channel;

	/**
	 * @var Database connection
	 */
	protected $conn;	// PDO connection handle

	protected $from;	// request parameters
	protected $to;		// can be NULL!
	protected $groupBy;	// user from/to from DataIterator for exact calculations!
	protected $client;  // client type for specific optimizations

	protected $useAggregation;  // use aggregation table for grouped queries

	protected $rowCount;	// number of rows in the database
	protected $tupleCount;	// number of requested tuples
	protected $rows;	// DataIterator instance for aggregating rows

	protected $min = NULL;
	protected $max = NULL;

	/**
	 * Constructor
	 *
	 * @param Channel $channel
	 * @param EntityManager $em
	 * @param integer $from timestamp in ms since 1970
	 * @param integer $to timestamp in ms since 1970
	 */
	public function __construct(Model\Channel $channel, ORM\EntityManager $em, $from, $to, $tupleCount, $groupBy, $client = 'unknown') {
		$this->channel = $channel;
		$this->groupBy = $groupBy;
		$this->tupleCount = $tupleCount;
		$this->client = $client;
		$this->conn = $em->getConnection(); // get dbal connection from EntityManager

		// parse interval
		if (isset($to))
			$this->to = self::parseDateTimeString($to);

		if (isset($from))
			$this->from = self::parseDateTimeString($from);
		else
			$this->from = ($this->to ? $this->to : time()*1000) - 24*60*60*1000; // default: "to" or now minus 24h

		if (isset($this->from) && isset($this->to) && $this->from > $this->to) {
			throw new \Exception('from is larger than to parameter');
		}

		// check if aggregation table can be used
		$this->useAggregation = isset($this->groupBy) && Util\Configuration::read('aggregation') &&
			Util\Aggregation::isValidAggregationLevel($this->groupBy);
	}

	/**
	 * Get minimum
	 *
	 * @return array (0 => timestamp, 1 => value)
	 */
	public function getMin() {
		return ($this->min) ? array_map('floatval', array_slice($this->min, 0 , 2)) : NULL;
	}

	/**
	 * Get maximum
	 *
	 * @return array (0 => timestamp, 1 => value)
	 */
	public function getMax() {
		return ($this->max) ? array_map('floatval', array_slice($this->max, 0 , 2)) : NULL;
	}

	/**
	 * Calculate valid timestamp boundaries for aggregation table usage in grouped queries
	 *
	 *     table:   --data-- -----aggregate----- -data-
	 * timestamp:   from ... agg_from ... agg_to ... to
	 *
	 * @param string $type aggregation level (e.g. 'day')
	 * @return boolean true: aggregate table contains data, agg_from/agg_to contains valid range
	 * @todo fix date format and interval calculation for aggregation modes different from 'day'
	 * @author Andreas Goetz <cpuidle@gmx.de>
	 */
	private function getAggregationBoundary($level, &$agg_from, &$agg_to) {
		$type = Util\Aggregation::getAggregationLevelTypeValue($level);
		$dateFormat = Util\Aggregation::getAggregationDateFormat($level); // day = "%Y-%m-%d"

		// agg_from becomes beginning of first period with aggregate data
		$sqlParameters = array($this->channel->getId(), $type, $this->from);
		$sql = 'SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(MIN(timestamp) / 1000, ' . $dateFormat . ')) * 1000 ' .
		 	   'FROM aggregate WHERE channel_id=? AND type=? AND timestamp>=?';
		$agg_from = $this->conn->fetchColumn($sql, $sqlParameters, 0);

		// aggregate table contains relevant data?
		if ($agg_from) {
			// agg_to becomes beginning of first period without aggregate data
			$sqlParameters = array($this->channel->getId(), $type);
			$sql = 'SELECT UNIX_TIMESTAMP(DATE_ADD(' .
						'FROM_UNIXTIME(MAX(timestamp) / 1000, ' . $dateFormat . '), ' .
						'INTERVAL 1 ' . $level . ')) * 1000 ' .
				   'FROM aggregate WHERE channel_id=? AND type=?';
			if (isset($this->to)) {
				$sqlParameters[] = $this->to;
				$sql .= ' AND timestamp<?';
			}
			$agg_to = $this->conn->fetchColumn($sql, $sqlParameters, 0);
		}

		return (isset($agg_from) && isset($agg_to));
	}

	/**
	 * Get raw data
	 *
	 * @param string|integer $groupBy
	 * @return Volkszaehler\DataIterator
	 */
	protected function getData() {
		if ($this->client !== 'raw') {
			// get timestamps of preceding and following data points as a graciousness
			// for the frontend to be able to draw graphs to the left and right borders
			if (isset($this->from)) {
				$sql = 'SELECT MIN(timestamp) FROM (SELECT timestamp FROM data WHERE channel_id=? AND timestamp<? ORDER BY timestamp DESC LIMIT 2) t';
				$from = $this->conn->fetchColumn($sql, array($this->channel->getId(), $this->from), 0);
				if ($from)
					$this->from = $from;
			}
			if (isset($this->to)) {
				$sql = 'SELECT MAX(timestamp) FROM (SELECT timestamp FROM data WHERE channel_id=? AND timestamp>? ORDER BY timestamp ASC LIMIT 2) t';
				$to = $this->conn->fetchColumn($sql, array($this->channel->getId(), $this->to), 0);
				if ($to)
					$this->to = $to;
			}
		}

		// common conditions for following SQL queries
		$sqlParameters = array($this->channel->getId());
		$sqlTimeFilter = self::buildDateTimeFilterSQL($this->from, $this->to, $sqlParameters);

		if ($this->groupBy) {
			$sqlGroupFields = self::buildGroupBySQL($this->groupBy);
			if (!$sqlGroupFields)
				throw new \Exception('Unknown group');

			$sqlRowCount = 'SELECT COUNT(DISTINCT ' . $sqlGroupFields . ') FROM data WHERE channel_id = ?' . $sqlTimeFilter;
			$sql = 'SELECT MAX(timestamp) AS timestamp, ' . static::groupExprSQL('value') . ' AS value, COUNT(timestamp) AS count'.
				' FROM data' .
				' WHERE channel_id = ?' . $sqlTimeFilter .
				' GROUP BY ' . $sqlGroupFields .
				' ORDER BY timestamp ASC';

			// Optimize grouped queries by applying aggregation table
			// @TODO fix sqlRowCount statement (if necessary)
			if ($this->useAggregation) {
				// numeric value of desired aggregation mode
				// @TODO add optimizer to choose best aggregation mode (for now, always 'day')
				$aggregationLevel = 'day';
				$type = Util\Aggregation::getAggregationLevelTypeValue($aggregationLevel);

				// calculate timestamp boundaries for aggregation table usage
				//     table:   --data-- -----aggregate----- -data-
				// timestamp:   from ... agg_from ... agg_to ... to
				if ($this->getAggregationBoundary($aggregationLevel, $agg_from, $agg_to)) {
					// NOTE: the UNION'ed tables are not ordered as MySQL doesn't guarantee result ordering
					$sqlParameters2 = array($this->channel->getId());
					// 	   table:   --DATA-- -----aggregate----- -data-
					$sqlTimeFilterPre = self::buildDateTimeFilterSQL($this->from, $agg_from, $sqlParameters2, true, '');
					// 	   table:   --data-- -----aggregate----- -DATA-
					$sqlTimeFilterPost = self::buildDateTimeFilterSQL($agg_to, $this->to, $sqlParameters2, false, '');
					$sql = 'SELECT timestamp, value, 1 AS count ' .
						   'FROM data ' .
						   'WHERE channel_id = ? ' .
						   'AND (' . $sqlTimeFilterPre . ' OR' . $sqlTimeFilterPost . ')';
					// 	   table:   --data-- -----AGGREGATE----- -data-
					array_push($sqlParameters2, $this->channel->getId(), $type);
					$sqlTimeFilter = self::buildDateTimeFilterSQL($agg_from, $agg_to, $sqlParameters2, true);
					$sql.=' UNION SELECT timestamp, value, count ' .
						   'FROM aggregate ' .
						   'WHERE channel_id = ? AND type = ?' . $sqlTimeFilter;

					// add common aggregation and sorting on UNIONed table
					$sql = 'SELECT MAX(timestamp) AS timestamp, SUM(value) AS value, SUM(count) AS count ' .
						   'FROM (' . $sql . ') AS agg ' .
						   'GROUP BY ' . $sqlGroupFields . ' ORDER BY timestamp ASC';
				}
			}
		}
		else {
			$sqlRowCount = 'SELECT COUNT(*) FROM data WHERE channel_id = ?' . $sqlTimeFilter;
			$sql = 'SELECT timestamp, value, 1 AS count FROM data WHERE channel_id=?' . $sqlTimeFilter . ' ORDER BY timestamp ASC';
		}

		$this->rowCount = (int) $this->conn->fetchColumn($sqlRowCount, $sqlParameters, 0);
		if ($this->rowCount <= 0)
			return new \EmptyIterator();

		// echo(Util\Debug::getParametrizedQuery($sql, isset($sqlParameters2) ? $sqlParameters2 : $sqlParameters)."\n");
		// file_put_contents("debug.log", Util\Debug::getParametrizedQuery($sql, isset($sqlParameters2) ? $sqlParameters2 : $sqlParameters)."\n", FILE_APPEND);

		// perform any optimization and run query
		$stmt = $this->runSQL($sql, isset($sqlParameters2) ? $sqlParameters2 : $sqlParameters);

		return new DataIterator($stmt, $this->rowCount, $this->tupleCount);
	}

	/**
	 * Execute SQL after performing potential optimizations
	 * Helper function to avoid duplicate code in derived classes
	 *
	 * Reduces number of tuples returned from DB if possible,
	 * basically does what DataIterator->next does when bundling tuples into packages
	 *
	 * @author Andreas Götz <cpuidle@gmx.de>
	 * @param string $sql
	 * @param string $sqlParameters
	 */
	protected function runSQL($sql, $sqlParameters) {
		// potential to reduce result set - can't do this for already grouped SQL
		if (!$this->groupBy && $this->tupleCount && ($this->rowCount > $this->tupleCount)) {
			$packageSize = floor($this->rowCount / $this->tupleCount);

			if ($packageSize > 1) { // worth doing -> go
				$foo = array();
				$sqlTimeFilter = self::buildDateTimeFilterSQL($this->from, $this->to, $foo);

				$this->rowCount = floor($this->rowCount / $packageSize);
				// setting @row to packageSize-2 will make the first package contain 1 tuple only - as it's skipped anyway
				// this pushes as much 'real' data as possible into the first used package
				$this->conn->query('SET @row:=' . ($packageSize-2));
				$sql = 'SELECT MAX(aggregate.timestamp) AS timestamp, ' .
							static::groupExprSQL('aggregate.value') .' AS value, COUNT(aggregate.value) AS count '.
					   'FROM ('.
					   '	SELECT timestamp, value, @row:=@row+1 AS row '.
					   ' 	FROM data WHERE channel_id=?' . $sqlTimeFilter .
					   'ORDER BY timestamp ) AS aggregate '.
					   'GROUP BY row DIV ' . $packageSize .' '.
					   'ORDER BY timestamp ASC';
			}
		}

		$stmt = $this->conn->executeQuery($sql, $sqlParameters); // query for data

		return($stmt);
	}

	/**
	 * Return sql grouping expression
	 *
	 * @author Andreas Götz <cpuidle@gmx.de>
	 * @param string $expression sql parameter
	 * @return string grouped sql expression
	 */
	protected static function groupExprSQL($expression) {
		return 'SUM(' . $expression . ')';
	}

	/**
	 * Builds sql query part for grouping data by date functions
	 *
	 * @param string $groupBy
	 * @return string the sql part
	 * @todo make compatible with: MSSql (Transact-SQL), Sybase, Firebird/Interbase, IBM, Informix, MySQL, Oracle, DB2, PostgreSQL, SQLite
	 */
	public static function buildGroupBySQL($groupBy) {
		$ts = 'FROM_UNIXTIME(timestamp/1000)';	// just for saving space

		switch ($groupBy) {
			case 'year':
				return 'YEAR(' . $ts . ')';
				break;

			case 'month':
				return 'YEAR(' . $ts . '), MONTH(' . $ts . ')';
				break;

			case 'week':
				return 'YEAR(' . $ts . '), WEEKOFYEAR(' . $ts . ')';
				break;

			case 'day':
				return 'YEAR(' . $ts . '), DAYOFYEAR(' . $ts . ')';
				break;

			case 'hour':
				return 'YEAR(' . $ts . '), DAYOFYEAR(' . $ts . '), HOUR(' . $ts . ')';
				break;

			case 'minute':
				return 'YEAR(' . $ts . '), DAYOFYEAR(' . $ts . '), HOUR(' . $ts . '), MINUTE(' . $ts . ')';
				break;

			case 'second':
				return 'YEAR(' . $ts . '), DAYOFYEAR(' . $ts . '), HOUR(' . $ts . '), MINUTE(' . $ts . '), SECOND(' . $ts . ')';
				break;

			default:
				return FALSE;
		}
	}

	/**
	 * Build sql query part to filter specified time interval
	 *
	 * @param integer $from timestamp in ms since 1970
	 * @param integer $to timestamp in ms since 1970
	 * @param boolean $sequential use < operator instead of <= for time comparison at end of period
	 * @param string  $op initial concatenation operator
	 * @return string the sql part
	 */
	protected static function buildDateTimeFilterSQL($from = NULL, $to = NULL, &$parameters, $sequential = false, $op = ' AND') {
		$sql = '';

		if (isset($from)) {
			$sql .= $op . ' timestamp >= ?';
			$parameters[] = $from;
		}

		if (isset($to)) {
			$sql .= (($sql) ? ' AND' : $op) . ' timestamp ' . (($sequential) ? '<' : '<=') . ' ?';
			$parameters[] = $to;
		}

		return $sql;
	}

	/**
	 * Parses a timestamp
	 *
	 * @link http://de3.php.net/manual/en/datetime.formats.php
	 * @todo add millisecond resolution
	 *
	 * @param string $ts string to parse
	 * @param float $now in ms since 1970
	 * @return float
	 */
	protected static function parseDateTimeString($string) {
		if (ctype_digit($string)) { // handling as ms timestamp
			return (float) $string;
		}
		elseif ($ts = strtotime($string)) {
			return $ts * 1000;
		}
		else {
			throw new \Exception('Invalid time format: \'' . $string . '\'');
		}
	}

	/*
	 * Getter & setter
	 */

	public function getEntity() { return $this->channel; }
	public function getRowCount() { return $this->rowCount; }
	public function getTupleCount() { return $this->tupleCount; }
	public function setTupleCount($count) { $this->tupleCount = $count; }
	public function getFrom() { return ($this->rowCount > 0) ? $this->rows->getFrom() : NULL; }
	public function getTo() { return ($this->rowCount > 0) ? $this->rows->getTo() : NULL; }
}

?>
