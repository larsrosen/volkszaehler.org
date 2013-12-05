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
	static $nodata;

	const TEST_DAYS = 365;		// count
	const TEST_SPACING = 60;	// sec

	const MSG_WIDTH = 20;

	private $min;
	private $max;

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

		self::$uuid = '9213ffb0-5dbc-11e3-a6df-8b30fe80a9e6';

		self::$testSize = round(self::TEST_DAYS * 24 * 3600 / self::TEST_SPACING);

		if (!self::$uuid)
			self::$uuid = self::createChannel('Aggregation', 'power', 100);
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
		if (self::$conn) {
			$agg = new Util\Aggregation(self::$conn);
			$agg->clear();
		}
		// parent::tearDownAfterClass();
	}

	protected function countAggregationRows($uuid = null) {
		return self::$conn->fetchColumn(
			'SELECT COUNT(aggregate.id) FROM aggregate ' .
			'LEFT JOIN entities ON aggregate.channel_id = entities.id ' .
			'WHERE entities.uuid = ?', array(($uuid) ?: self::$uuid)
		);
	}

	protected function clearCache() {
		self::$conn->executeQuery('FLUSH QUERY CACHE');
		// self::$conn->executeQuery('FLUSH STATUS, TABLES WITH READ LOCK');
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

	function testClearAggregation() {
		$agg = new Util\Aggregation(self::$conn);
		$agg->clear();

		$rows = self::$conn->fetchColumn('SELECT COUNT(id) FROM aggregate');
		$this->assertEquals(0, $rows, 'aggregate table cannot be cleared');
	}

	function testPrepareData() {
		$channel = self::getChannelByUUID(self::$uuid);
		$channel_id = $channel->getId();

		return;

		$this->msg('TestSize', self::$testSize);

		self::$em->getConnection()->beginTransaction();
		$time = microtime(true);
		for ($i=0; $i<self::$testSize; $i++) {
			$ts = $i * 1000 * self::TEST_SPACING;
			$val = 10;

			self::$conn->executeQuery(
				'INSERT DELAYED INTO data (channel_id, timestamp, value) VALUES(?, ?, ?)',
				array($channel_id, $ts, $val));
		}
		$time = microtime(true) - $time;
		self::$em->getConnection()->commit();

		$this->timer($time, "AddTime");
	}

	function testGetAllData() {
		$this->min = 0;
		$this->max = self::$testSize * self::TEST_SPACING * 1000;

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max);

		$this->perf($time, "GetPerf");
	}

	function testGetAllDataGrouped() {
		file_put_contents("1.txt", "reset query cache;\n\n");
		file_put_contents("1.txt", self::$uuid."\n\n", FILE_APPEND);

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max, "day");
		$time = microtime(true) - $time;

		$this->perf($time, "GetGroupPerf");
	}

	function testAggregation() {
		$agg = new Util\Aggregation(self::$conn);
		$agg->clear();

		$time = microtime(true);
		$agg->aggregate('full', 'day', null, self::getChannelByUUID(self::$uuid)->getId());
		$time = microtime(true) - $time;
		$this->timer($time, "AggTime");

		$rows = $this->countAggregationRows();
		$this->assertGreaterThan(0, $rows);
		echo($this->msg("AggRatio", "1:" . round(self::$testSize / $rows)));

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max, "day");
		$time = microtime(true) - $time;

		$this->perf($time, "GetAggPerf");
	}
}

?>
