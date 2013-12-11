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
	static $to; // limit data set for low performance clients
	static $base;
	static $baseline;

	const TEST_DAYS = 365;		 // count
	const TEST_SPACING = 60;	 // sec
	const MSG_WIDTH = 30;		 // output width

	private $time;

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

		if (!self::$uuid) {
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

		self::$testSize = round(self::TEST_DAYS * 24 * 3600 / self::TEST_SPACING);
		self::$base = self::$context . '/' . static::$uuid . '.json?';
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

	private function perf($msg, $speedup = false) {
		$time = microtime(true) - $this->time;
		if (!$speedup) self::$baseline = $time;
		$timeStr = sprintf(($time >= 1000) ? "%.0f" : "%.2f", $time);
		echo($this->formatMsg($msg) . $timeStr . "s ");

		if ($speedup) {
			$speedup = self::$baseline / $time;
			echo('x' . sprintf("%.".max(0,2-floor(log($speedup,10)))."f", $speedup) . ' ');
		}
	}

	function setUp() {
		$this->clearCache();
		$this->time = microtime(true);
	}

	function testConfiguration() {
		$this->assertTrue(Util\Configuration::read('aggregation'), 'data aggregation not enabled in config file, set `config[aggregation] = true`');
	}

	function testAggregation() {
		$rows = $this->countAggregationRows(self::$uuid);
		$this->assertGreaterThan(0, $rows);
		echo($this->formatMsg("AggRatio") . "1:" . round(self::$testSize / $rows));
	}

	// function testGetAllData() {
	// 	$this->getTuplesByUrl(self::$base, 1, '1.2.2000', null, null, 'client=slow');
	// 	$this->perf("GetAllPerf");
	// }

	// function testGetAllData2() {
	// 	$this->getTuplesByUrl(self::$base, 1, '1.2.2000', null, null);
	// 	$this->perf("GetAllPerf (opt)", true);
	// }

	function testGetAllDataGrouped() {
		$this->getTuplesByUrl(self::$base, 1, self::$to, 'day', null, 'client=slow');
		$this->perf("GetGroupPerf");
	}

	function testGetAllDataGrouped2() {
		$this->getTuplesByUrl(self::$base, 1, self::$to, 'day', null);
		$this->perf("GetGroupPerf (opt)", true);
	}

	function testGetTotal() {
		$this->getTuplesByUrl(self::$base, 1, self::$to, null, 1, 'client=slow');
		$this->perf("GetTotalPerf");
	}

	function testGetTotal2() {
		$this->getTuplesByUrl(self::$base, 1, self::$to, null, 1);
		$this->perf("GetTotalPerf (opt)", true);
	}
}

?>
