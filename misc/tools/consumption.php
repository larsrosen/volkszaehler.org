<?php
/**
 * Command line tool for setting channel consumption to desired value
 *
 * Implemented by manipulating the first DB tuple total consumption
 *
 * @author Andreas Goetz <cpuidle@gmx.de>
 * @copyright Copyright (c) 2013, The volkszaehler.org project
 * @package tools
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

use Volkszaehler\Util;
use Volkszaehler\Definition;
use Volkszaehler\Controller;
use Volkszaehler\View;

define('VZ_DIR', realpath(__DIR__ . '/../../'));

require VZ_DIR . '/lib/bootstrap.php';

/**
* Total handling
*/
class Consumption
{
	protected $em;
	protected $conn;
	protected $view = null;

	private $uuid;
	private $total;
	private $pretty;
	private $verbose;

	private $entity;
	private $resolution;
	private $class;
	private $tuples;

	function __construct($uuid, $total, $pretty = false, $verbose = false)
	{
		$this->uuid = $uuid;
		$this->total = $total;
		$this->pretty = $pretty;
		$this->verbose = $verbose;

		$this->em = Volkszaehler\Router::createEntityManager();
		$this->conn = $this->em->getConnection();
	}

	private function numFmt($num) {
		return ($this->pretty) ? number_format($num, (abs($num) > 100) ? 0 : 2, '.', ',') : $num;
	}

	public function getTuples($from, $to, $group = null, $tuples = null, $options = null)
	{
		$interpreter = new $this->class($this->entity, $this->em, $from, $to, $group, $tuples, $options);
		// call to processData necessary to populate interpreter
		$this->tuples = $interpreter->processData(function($tuple) { return $tuple; });
		return $interpreter;
	}

	function run() {
		$ec = new Controller\EntityController($this->view, $this->em);
		$this->entity = $ec->get($this->uuid);
		$this->class = $this->entity->getDefinition()->getInterpreter();
		$this->resolution = $this->entity->getDefinition()->hasConsumption ? $this->entity->getProperty('resolution') : NULL;

		if (empty($this->resolution)) {
			echo("Channel does not support consumption");
			die;
		}

		// get current total
		$interpreter = $this->getTuples(0, '31.12.2030', 2);
		$consumption = $interpreter->getConsumption();
		$rowCount = $interpreter->getRowCount();
		echo("Current total: ".$this->numFmt($consumption)." Wh\n");
		// echo("rowCount: $rowCount\n");

		if ($this->total == $consumption) {
			echo("Nothing to do.");
			return;
		}

		if ($this->total && $rowCount > 1) {
			echo("New total: ".$this->numFmt($this->total)." Wh\n");

			// find first tuple
			$interpreter = $this->getTuples(0, 0);
			$from = $interpreter->getFrom();
			$to = (count($this->tuples) > 1) ? $this->tuples[1][0] : $interpreter->getTo();	// end of first period
			if ($this->verbose) echo("First period: ".(($to - $from)/1000)."s\n");

			// add consumption of first tuple
			if ($this->verbose) echo("Period usage: ".$this->numFmt($this->tuples[0][1])." W\n");
			$periodConsumption = $this->tuples[0][1] * ($to - $from) / 3.6e6;
			if ($this->verbose) echo("Period consumption: ".$this->numFmt($periodConsumption)." Wh\n");

			// new desired total consumption
			$newConsumption = $this->total - $consumption + $periodConsumption; // Wh
			if ($this->verbose) echo("Updated consumption: ".$this->numFmt($newConsumption)." Wh\n");

			$periodValue = $newConsumption * $this->resolution / 1000;
			if ($this->verbose) echo("Updated value: ".$this->numFmt($periodValue)."\n");

			// update and clean aggregate table
			$this->conn->beginTransaction();

			$sql = 'UPDATE data ' .
				   'INNER JOIN entities ON data.channel_id = entities.id ' .
				   'SET value = ? ' .
				   'WHERE entities.uuid = ? AND timestamp = ?';
			$this->conn->executeQuery($sql, array($periodValue, $this->uuid, $to));

			// clean aggregates- now invalid
			if (Util\Configuration::read('aggregation')) {
				$agg = new Util\Aggregation($this->conn);
				$agg->clear($this->uuid);

				echo("Aggregation is enabled- please recreate aggregation table:\n");
				echo("php misc/tools/aggregate.php -u " . $this->uuid . " run\n");
			}

			$this->conn->commit();

			// verification step- only if verbose as this can be slow
			if ($this->verbose) {
				$interpreter = $this->getTuples(0, '31.12.2030', 1);
				$consumption = $interpreter->getConsumption();
				echo("Updated total: ".$this->numFmt($consumption)."\n");
			}
		}
	}
}

$console = new Util\Console($argv, array('u:'=>'uuid:', 't:'=>'total:', 'p'=>'pretty', 'h'=>'help', 'v'=>'verbose'));

if ($console::isConsole()) {
	$help    = $console->getOption('h');
	$uuid    = $console->getOption('u');
	$total   = $console->getOption('t');
	$pretty  = $console->getOption('p');
	$verbose = $console->getOption('v');

	if ($help || empty($uuid)) {
		echo("Usage: consumption.php [options] value\n");
		echo("Options:\n");
		echo("           -u[uid] uuid\n");
		echo("          -t[otal] new total value\n");
		echo("         -p[retty] format output values\n");
		echo("        -v[erbose] verbose messages\n");
		echo("Example:\n");
		echo("       consumption.php --uuid ABCD-0123 -t 19000\n");
		die;
	}

	$job = new Consumption($uuid[0], $total[0], $pretty, $verbose);
	$job->run();
}
else
	throw new \Exception('This tool can only be run locally.');

?>
