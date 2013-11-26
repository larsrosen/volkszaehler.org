<?php
/**
 * Meter tests
 *
 * @package Test
 * @author Andreas Götz <cpuidle@gmx.de>
 */

namespace Tests;

class DataSensorTest extends DataContext
{
	/**
	 * Create channel
	 */
	static function setupBeforeClass() {
		parent::setupBeforeClass();
		if (!self::$uuid)
			self::$uuid = self::createChannel('Sensor', 'powersensor');
	}

	function testSensor() {
		echo('Not Implemented');
	}
}

?>
