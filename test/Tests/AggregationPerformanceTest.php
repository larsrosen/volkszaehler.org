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

	const TEST_SIZE = 1000;		// 1 day
	const TEST_SPACING = 60;	// 5 min
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
		// if (self::$conn) {
		// 	$agg = new Util\Aggregation(self::$conn);
		// 	$agg->clear();
		// }
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

	function formatMsg($msg) {
		$msg = "\n" . $msg . ' ';
		while (strlen($msg) < self::MSG_WIDTH) {
			$msg .= '.';
		}
		return $msg  . ' ';
	}

	function msg($msg, $val = null) {
		echo($this->formatMsg($msg));
		echo($val . " ");
	}

	function perf($time, $msg = null) {
		if ($msg) {
			echo($this->formatMsg($msg));
		}
		echo((self::TEST_SIZE / $time) . " recs/s ");
	}

	function timer($time, $msg = null) {
		if ($msg) {
			echo($this->formatMsg($msg));
		}
		echo(($time) . " s ");
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
		self::$em->getConnection()->beginTransaction();

		$time = microtime(true);
		for ($i=0; $i<self::TEST_SIZE; $i++) {
			$ts = $i * 1000 * self::TEST_SPACING;
			$val = 10;

			// $this->addTuple($ts, $val);
			$channel->addData(new Model\Data($channel, $ts, $val));
		}
		self::$em->flush();
		self::$em->getConnection()->commit();
		$time = microtime(true) - $time;

		$this->timer($time, "AddTime");
	}

	function testGetAllData() {
		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max);

		$this->perf($time, "GetPerf");
	}

	function testGetAllDataGrouped() {
		file_put_contents("1.txt", "reset query cache;\n\n");

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max, "day");
		$time = microtime(true) - $time;

		$this->perf($time, "GetGroupPerf");
	}

	/**
	 * @depends testClearAggregation
	 */
	function testAggregation() {
		$agg = new Util\Aggregation(self::$conn);
		$agg->clear();

		$time = microtime(true);
		$agg->aggregate('full', 'day', null, self::getChannelByUUID(self::$uuid)->getId());
		$time = microtime(true) - $time;
		$this->timer($time, "AggTime");

		$rows = $this->countAggregationRows();
		$this->assertGreaterThan(0, $rows);
		echo($this->msg("AggRatio", "1:" . round(self::TEST_SIZE / $rows)));

		$this->clearCache();
		$time = microtime(true);
		$this->getTuples($this->min, $this->max, "day");
		$time = microtime(true) - $time;

		$this->perf($time, "GetAggPerf");
	}
}

?>
