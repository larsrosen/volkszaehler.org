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

// use GetOptionKit\GetOptionKit;

/**
 * @author Andreas Goetz <cpuidle@gmx.de>
 */
class Cron {
	/**
	 * @var \Doctrine\ORM\EntityManager Doctrine EntityManager
	 */
	protected $em;

	public function __construct() {
		$this->em = Router::createEntityManager(true); // get admin credentials
	}

	public function run($command, $uuid, $levels, $mode, $period = NULL) {
		$aggregator = new Util\Aggregation($this->em->getConnection());

		if (!in_array($mode, array('full', 'delta')))
			throw new \Exception('Unsupported aggregation mode ' . $mode);

		$conn = $this->em->getConnection();

		if ($command == 'create') {
			echo("Recreating aggregation table.\n");
			$conn->executeQuery('DROP TABLE IF EXISTS `aggregate`');
			$conn->executeQuery(
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
		elseif ($command == 'clear') {
			$msg = "Clearing aggregation table";
			if ($uuid) $msg .= " for UUID " . $uuid;
			echo($msg . ".\n");
			$aggregator->clear($uuid);
			echo("Done clearing aggregation table.\n");
		}
		elseif ($command == 'aggregate' || $command == 'run') {
			// loop through all aggregation levels
			foreach ($levels as $level) {
				if (!Util\Aggregation::isValidAggregationLevel($level))
					throw new \Exception('Unsupported aggregation level ' . $level);

				$msg = "Performing '" . $mode . "' aggregation";
				if ($uuid) $msg .= " for UUID " . $uuid;
				echo($msg . " on '" . $level . "' level.\n");
				$rows = $aggregator->aggregate($uuid, $level, $mode, $period);
				echo("Updated $rows rows.\n");
			}
		}
		elseif ($command == 'optimize') {
			echo("Optimizing aggregate table.\n");
			$conn->executeQuery('OPTIMIZE TABLE aggregate');
			echo("Optimizing data table (slow).\n");
			$conn->executeQuery('OPTIMIZE TABLE data');
		}
		else
			throw new \Exception('Unknown command ' . $command);
	}
}

// TODO fix parameter splitting and add help
if (php_sapi_name() == 'cli' || isset($_SERVER['SESSIONNAME']) && $_SERVER['SESSIONNAME'] == 'Console') {
	// parse options
	$options = getopt("u:m:l:p:h", array('uuid:', 'mode:', 'level:', 'period:', 'help'));

	$commands = array();
	for ($i=1; $i<count($argv); $i++) {
		$arg = $argv[$i];
		// echo("$arg\n");
		// skip following parameter if option
		if (preg_match('#^[\-/].#', $arg)) {
			if (isset($options[substr($arg, 1)])) {
				// echo("-> ".print_r($options[substr($arg, 1)],1));
				$i++;
			}
			// echo("-> cont\n");
			continue;
		}
		$commands[] = $arg;
	}

	if (isset($options['h']) || count($commands) == 0) {
		echo("Usage: aggregate.php [options] command[s]\n");
		echo("Commands:\n");
		echo("       aggregate|run Run aggregation\n");
		echo("              create Create aggregation table (DESTRUCTIVE)\n");
		echo("               clear Clear aggregation table\n");
		echo("            optimize Opimize data and aggregate tables\n");
		echo("Options:\n");
		echo("             -u[uid] uuid\n");
		echo("            -l[evel] hour|day|month|year\n");
		echo("             -m[ode] full|delta\n");
		echo("           -p[eriod] number of previous time periods\n");
		echo("Example:\n");
		echo("         aggregate.php --uuid ABCD-0123 --mode delta -l month\n");
		echo("Create monthly and daily aggregation data since last run for specified UUID\n");
	}

	$uuid    = (isset($options['u'])) ? strtolower($options['u']) : null;
	$mode    = (isset($options['m'])) ? strtolower($options['m']) : 'delta';
	$period  = (isset($options['p'])) ? intval($options['p']) : 0;
	$level   = (isset($options['l'])) ? (is_array($options['l']) ? $options['l'] : array($options['l'])) : array('day');

 	// $getopt = new GetOptionKit;
	// $getopt->add('u|uuid:', 'uuid');
	// $getopt->add('l|level:', 'hour|day|month|year');
	// $getopt->add('m|mode:', 'full|delta');
	// $getopt->add('p|period:', 'number of previous time periods');

	// try {
	//  	$result = $getopt->parse( $argv );
	//  	print_r($result);
	//  	$result->verbose;
	//  	$result->debug;
	// } catch( Exception $e ) {
	// 	echo $e->getMessage();
	// }

	$cron = new Cron();

	foreach ($commands as $command) {
		$cron->run($command, $uuid, $level, $mode, $period);
	}
}
else
	throw new \Exception('This tool can only be run locally.');

?>
