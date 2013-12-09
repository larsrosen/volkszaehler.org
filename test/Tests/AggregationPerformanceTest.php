<?php
/**
 * Aggregation tests
 *
 * NOTE: these tests should be DST-ready
 *
 * @package Test
 * @author Andreas Götz <cpuidle@gmx.de>
 */

namespace Tests;

use Volkszaehler\Util;
use Volkszaehler\Model;
use Doctrine\DBAL;

class AggregationPerformanceTest extends DataContext
{
	static $conn;
	static $em;

	static $testSize;
	static $uuid = '00000000-0000-0000-0000-000000000000';
	static $to; // = '1.2.2000'; // limit data set for low performance clients

	const TEST_DAYS = 365;		 // count
	const TEST_SPACING = 60;	 // sec

	const MSG_WIDTH = 20;

	/**
	 * Create DB connection and setup channel
	 */
	static function setupBeforeClass() {
		parent::setupBeforeClass();

		$dbConfig = Util\Configuration::read('db');
		if (isset($dbConfig['admin'])) {
			$dbConfig = array_merge($dbConfig, $dbConfig['admin']);
		}
		self::$conn = DBAL\DriverManager::getConnection($dbConfig);
		self::$em = \Volkszaehler\Router::createEntityManager();

		self::$testSize = round(self::TEST_DAYS * 24 * 3600 / self::TEST_SPACING);

		if (!self::$uuid) {
			// self::$uuid = self::createChannel('Aggregation', 'power', 100);
			echo("Failure: need UUID before test.\nRun `phpunit Tests\SetupPerformanceData` to generate.");
			die;
		}

		if (isset(self::$to)) {
			if (!is_numeric(self::$to)) {
				self::$to = strtotime(self::$to) * 1000;
			}
		}
		else {
			self::$to = null;
		}
	}

	static function getChannelByUUID($uuid) {
		$dql = 'SELECT a, p
			FROM Volkszaehler\Model\Entity a
			LEFT JOIN a.properties p
			WHERE a.uuid = :uuid';

		$q = self::$em->createQuery($dql);
		$q->setParameter('uuid', $uuid);

		return $q->getSingleResult();
	}

	/**
	 * Cleanup aggregation
	 */
	static function tearDownAfterClass() {
		// keep channel
	}

	protected function countAggregationRows($uuid = null) {
		return self::$conn->fetchColumn(
			'SELECT COUNT(aggregate.id) FROM aggregate ' .
			'LEFT JOIN entities ON aggregate.channel_id = entities.id ' .
			'WHERE entities.uuid = ?', array(($uuid) ?: self::$uuid)
		);
	}

	protected function clearCache() {
		self::$conn->executeQuery('FLUSH TABLES');
		self::$conn->executeQuery('RESET QUERY CACHE');
	}

	private function formatMsg($msg) {
		$msg = "\n" . $msg . ' ';
		while (strlen($msg) < self::MSG_WIDTH) {
			$msg .= '.';
		}
		return $msg  . ' ';
	}

	private function formatVal($val) {
		if (is_float($val)) {
			return(sprintf(($val >= 1000) ? "%.0f" : "%.2f", $val));
		}
		return $val;
	}

	private function msg($msg, $val = null) {
		echo($this->formatMsg($msg));
		echo($val . " ");
	}

	private function perf($time, $msg = null) {
		if ($msg) {
			echo($this->formatMsg($msg));
		}
		echo($this->formatVal(self::$testSize / $time) . " recs/s ");
	}

	private function timer($time, $msg = null) {
		if ($msg) {
			echo($this->formatMsg($msg));
		}
		echo($this->formatVal($time) . " s ");
	}

	function testConfiguration() {
		$this->assertTrue(Util\Configuration::read('aggregation'), 'data aggregation not enabled in config file, set `config[aggregation] = true`');
	}

	function testGetAllDataGrouped() {
		$this->clearCache();
		$time = microtime(true);
		$this->getTuples(1, self::$to, "day");
		$time = microtime(true) - $time;

		$this->perf($time, "GetGroupPerf");
	}

	function testAggregation() {
		$channel_id = self::getChannelByUUID(self::$uuid)->getId();

		$rows = $this->countAggregationRows(self::$uuid);
		$this->assertGreaterThan(0, $rows);
		echo($this->msg("AggRatio", "1:" . round(self::$testSize / $rows)));
	}

	function testGetAllDataGroupedAggregated() {
		$this->clearCache();
		$time = microtime(true);
		// $this->getTuples(1, null, 'day');
		$url = self::$context . '/' . static::$uuid . '.json?';
		$this->getTuplesByUrl($url, 1, self::$to, 'day', null, 'client=agg');
		$time = microtime(true) - $time;

		$this->perf($time, "GetAggPerf");
	}

	function testGetTotal() {
		$this->clearCache();
		$time = microtime(true);
		$this->getTuples(1, self::$to, null, 1);
		$time = microtime(true) - $time;

		$this->perf($time, "GetTotalPerf");

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples(1, self::$to, 'day', 1);
		$time = microtime(true) - $time;

		$this->perf($time, "GetTotalGroup");

		$this->clearCache();
		$time = microtime(true);
		// $this->getTuples(1, null, 'day');
		$url = self::$context . '/' . static::$uuid . '.json?';
		$this->getTuplesByUrl($url, 1, self::$to, 'day', 1, 'client=agg');
		$time = microtime(true) - $time;

		$this->perf($time, "GetTotalAgg");
	}
}

?>
