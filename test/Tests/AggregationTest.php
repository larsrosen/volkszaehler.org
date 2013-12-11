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
use Doctrine\DBAL;

class AggregationTest extends DataContext
{
	static $conn;

	/**
	 * Create DB connection and setup channel
	 */
	static function setupBeforeClass() {
		parent::setupBeforeClass();
		self::$conn = DBAL\DriverManager::getConnection(Util\Configuration::read('db'));

		if (!self::$uuid)
			self::$uuid = self::createChannel('Aggregation', 'power', 100);
	}

	/**
	 * Cleanup aggregation
	 */
	static function tearDownAfterClass() {
		if (self::$conn && self::$uuid) {
			$agg = new Util\Aggregation(self::$conn);
			$agg->clear(self::$uuid);
		}
		parent::tearDownAfterClass();
	}

	protected function countAggregationRows($uuid = null) {
		return self::$conn->fetchColumn(
			'SELECT COUNT(aggregate.id) FROM aggregate ' .
			'LEFT JOIN entities ON aggregate.channel_id = entities.id ' .
			'WHERE entities.uuid = ?', array(($uuid) ?: self::$uuid)
		);
	}

	function testConfiguration() {
		$this->assertTrue(Util\Configuration::read('aggregation'), 'data aggregation not enabled in config file, set `config[aggregation] = true`');
	}

	function testClearAggregation() {
		$agg = new Util\Aggregation(self::$conn);
		$agg->clear(self::$uuid);

		$rows = self::$conn->fetchColumn('SELECT COUNT(1) FROM aggregate INNER JOIN entities ON aggregate.channel_id = entities.id WHERE entities.id = ?', array(self::$uuid));
		$this->assertEquals(0, $rows, 'aggregate table cannot be cleared');
	}

	/**
	 * @depends testClearAggregation
	 */
	function testDeltaAggregation() {
		$agg = new Util\Aggregation(self::$conn);

		// 0:00 today current timezone - must not be aggregated
		$this->addTuple(strtotime('today 0:00') * 1000, 50);
		$agg->aggregate(self::$uuid, 'day', 'delta', 2);

		$rows = $this->countAggregationRows();
		$this->assertEquals(0, $rows, 'current period wrongly appears in aggreate table');

		// 0:00 last two days - must be aggregated
		$this->addTuple(strtotime('1 days ago 0:00') * 1000, 100);
		$this->addTuple(strtotime('1 days ago 12:00') * 1000, 100);
		$this->addTuple(strtotime('2 days ago 0:00') * 1000, 100);
		$this->addTuple(strtotime('2 days ago 12:00') * 1000, 100);
		$agg->aggregate(self::$uuid, 'day', 'delta', 2);

		$rows = $this->countAggregationRows();
		$this->assertEquals(2, $rows, 'last period missing from aggreate table');

		// 0:00 three days ago - must not be aggregated
		$this->addTuple(strtotime('3 days ago 0:00') * 1000, 50);
		$agg->aggregate(self::$uuid, 'day', 'delta', 2);

		$rows = $this->countAggregationRows();
		$this->assertEquals(2, $rows, 'period before last wrongly appears in aggreate table');
	}

	/**
	 * @depends testDeltaAggregation
	 */
	function testDeltaAggregationSecondChannel() {
		$agg = new Util\Aggregation(self::$conn);

		// create 2nd channel
		$uuid2 = self::createChannel('AggregationSecondChannel', 'power', 100);

		// 0:00 last yesterday - must be aggregated
		$this->addTuple(strtotime('1 days ago 0:00') * 1000, 100, $uuid2);
		$agg->aggregate($uuid2, 'day', 'delta', 2);

		$rows = $this->countAggregationRows($uuid2);
		$this->assertEquals(1, $rows, 'repeated delta aggregation failed');

		// cleanup 2nd channel
		self::deleteChannel($uuid2);
	}

	/**
	 * @depends testClearAggregation
	 * @depends testDeltaAggregation
	 * @depends testConfiguration
	 */
	function testGetBaseline() {
		$agg = new Util\Aggregation(self::$conn);
		$agg->clear(self::$uuid);

		// unaggregated datapoints - 6 rows
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000);
		$this->assertEquals(6, $this->json->data->rows);

		// unaggregated datapoints grouped - 4 rows for comparison
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, null, 'day');
		$this->assertEquals(4, $this->json->data->rows);

		// save baseline, then aggregate
		$tuples = $this->json->data->tuples;
		$agg->aggregate(self::$uuid, 'day', 'delta', 2);

		// aggregated datapoints grouped - 4 rows for comparison
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, null, 'day');
		$this->assertEquals(4, $this->json->data->rows);

		foreach($this->json->data->tuples as $tuple) {
			$t = array_shift($tuples);
			$this->assertTuple($t, $tuple);
		}
	}

	/**
	 * @depends testGetBaseline
	 */
	function testAggregateRetrievalFrom() {
		// 1 data
		$this->getTuplesRaw(strtotime('today 0:00') * 1000, null, 'day');
		$this->assertEquals(1, $this->json->data->rows);

		// 1 agg + 1 data
		$this->getTuplesRaw(strtotime('1 days ago 0:00') * 1000, null, 'day');
		$this->assertEquals(2, $this->json->data->rows);

		//  1 agg + 1 agg + 1 data
		$this->getTuplesRaw(strtotime('2 days ago 0:00') * 1000, null, 'day');
		$this->assertEquals(3, $this->json->data->rows);

		// 1 data + 1 agg + 1 agg + 1 data
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, null, 'day');
		$this->assertEquals(4, $this->json->data->rows);
	}

	/**
	 * @depends testGetBaseline
	 */
	function testAggregateRetrievalTo() {
		// 1 data + 1 agg + 1 agg + 1 data
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, strtotime('today 18:00') * 1000, 'day');
		$this->assertEquals(4, $this->json->data->rows);

		// 1 data + 1 agg + 1 data(aggregated)
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, strtotime('1 days ago 6:00') * 1000, 'day');
		$this->assertEquals(3, $this->json->data->rows);

		// 1 data + 1 data(aggregated)
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, strtotime('2 days ago 6:00') * 1000, 'day');
		$this->assertEquals(2, $this->json->data->rows);

		// 1 data
		$this->getTuplesRaw(strtotime('3 days ago 0:00') * 1000, strtotime('3 days ago 18:00') * 1000, 'day');
		$this->assertEquals(1, $this->json->data->rows);
	}

	function testFullAggregation() {
		// currently not implemented for performance reasons
		echo('currently not implemented for performance reasons');
	}
}

?>
