<?php
/**
 * Data aggregation command line tool
 *
 * To setup aggregation job run crontab -e
 * 0 0 * * * /usr/bin/php cron.php
 *
 * @author Andreas Goetz <cpuidle@gmx.de>
 * @copyright Copyright (c) 2013, The volkszaehler.org project
 * @package default
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

namespace Volkszaehler;

define('VZ_DIR', realpath(__DIR__ . '/../..'));

require_once VZ_DIR . '/lib/bootstrap.php';

/**
 * @author Andreas Goetz <cpuidle@gmx.de>
 */
class Cron {
	/**
	 * @var \Doctrine\ORM\EntityManager Doctrine EntityManager
	 */
	protected $em;

	/**
	 * @var Util\Aggregation Database aggregator
	 */
	protected $aggregator;

	public function __construct() {
		$this->em = Router::createEntityManager(true); // get admin credentials
		$this->aggregator = new Util\Aggregation($this->em->getConnection());
	}

	public function run($mode, $levels, $period = NULL) {
		if (!in_array($mode, array('create', 'clear', 'full', 'delta')))
			throw new \Exception('Unsupported aggregation mode ' . $mode);

		if ($mode == 'create') {
			echo("Recreating aggregation table.\n");
			$conn = $this->em->getConnection();

			$conn->exec('DROP TABLE IF EXISTS `aggregate`');
			$conn->exec(
				'CREATE TABLE `aggregate` (' .
				'  `id` int(11) NOT NULL AUTO_INCREMENT,' .
				'  `channel_id` int(11) NOT NULL,' .
				'  `type` tinyint(1) NOT NULL,' .
				'  `timestamp` bigint(20) NOT NULL,' .
				'  `value` double NOT NULL,' .
				'  `count` int(11) NOT NULL,' .
				'  PRIMARY KEY (`id`),' .
				'  UNIQUE KEY `ts_uniq` (`channel_id`,`type`,`timestamp`)' .
				')');
		}
		else if ($mode == 'clear') {
			echo("Clearing aggregation table.\n");
			$this->aggregator->clear();
			echo("Truncated aggregation table.\n");
		}
		else {
			// loop through all aggregation levels
			foreach ($levels as $level) {
				if (!Util\Aggregation::isValidAggregationLevel($level))
					throw new \Exception('Unsupported aggregation level ' . $level);

				echo("Performing $mode aggregation on $level level.\n");
				$rows = $this->aggregator->aggregate($mode, $level, $period);
				echo("Updated $rows rows.\n");
			}
		}
	}
}

// TODO fix parameter splitting and add help
if (php_sapi_name() == 'cli' || isset($_SERVER['SESSIONNAME']) && $_SERVER['SESSIONNAME'] == 'Console') {
	// parse options
	$options = getopt("m:l:p:h", array('mode:', 'level:', 'periods:', 'help'));
	$mode    = (isset($options['m'])) ? strtolower($options['m']) : 'delta';
	$level   = (isset($options['l'])) ? preg_split('/,/', strtolower($options['l'])) : array('day');
	$period  = (isset($options['p'])) ? intval($options['p']) : 0;

	$cron = new Cron();
	$cron->run($mode, $level, $period);
}
else
	throw new \Exception('This tool can only be run locally.');

?>
