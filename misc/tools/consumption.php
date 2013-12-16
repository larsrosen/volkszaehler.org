<?php

use Volkszaehler\Util;
use Volkszaehler\Definition;
use Volkszaehler\Controller;
use Volkszaehler\View;

define('VZ_DIR', realpath(__DIR__ . '/../../'));

require VZ_DIR . '/lib/bootstrap.php';

/**
* Total handling
*/
class SetTotal
{
	protected $em;
	protected $conn;
	protected $view = null;

	private $uuid;
	private $total;
	private $pretty;

	private $entity;
	private $resolution;
	private $class;
	private $tuples;

	function __construct($uuid, $total, $pretty = false)
	{
		$this->uuid = $uuid;
		$this->total = $total;
		$this->pretty = $pretty;

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

		if ($this->total && $rowCount > 1) {
			// find first tuple
			$interpreter = $this->getTuples(0, 0);
// print_r($this->tuples);
			$from = $interpreter->getFrom();
			$to = (count($this->tuples) > 1) ? $this->tuples[1][0] : $interpreter->getTo();	// end of first period
// echo("from/to $from $to\n");
			echo("Period: ".(($to - $from)/1000)."s\n");

			// add consumption of first tuple
			echo("periodValue: ".$this->numFmt($this->tuples[0][1])." W\n");
			$periodConsumption = $this->tuples[0][1] * ($to - $from) / 3.6e6;
			echo("periodConsumption: ".$this->numFmt($periodConsumption)." Wh\n");

			// new desired total consumption
			$newConsumption = $this->total - $consumption + $periodConsumption; // Wh
			echo("newConsumption: ".$this->numFmt($newConsumption)." Wh\n");

			$periodValue = $newConsumption * $this->resolution / 1000;
			echo("periodValue: ".$this->numFmt($periodValue)."\n");

			// update and clean aggregate table
			$this->conn->beginTransaction();

			$sql = 'UPDATE data ' .
				   'INNER JOIN entities ON data.channel_id = entities.id ' .
				   'SET value = ? ' .
				   'WHERE entities.uuid = ? AND timestamp = ?';
			$this->conn->executeQuery($sql, array($periodValue, $this->uuid, $to));

			// clean aggregates- now invalid
			if (Util\Configuration::read('aggregation')) {
				$sql = 'DELETE aggregate ' .
					   'FROM aggregate ' .
					   'INNER JOIN entities ON aggregate.channel_id = entities.id ' .
					   'WHERE entities.uuid = ?';
				$this->conn->executeQuery($sql, array($this->uuid));
			}

			$this->conn->commit();

			$interpreter = $this->getTuples(0, '31.12.2030', 1);
			$consumption = $interpreter->getConsumption();
			echo("Updated total: ".$this->numFmt($consumption)."\n");

			// $interpreter = $this->getTuples(0, 0);
			// print_r($this->tuples);
		}
	}
}

/**
 * Get short or long options value
 * @param  string $short short option name
 * @param  string $long  long option name
 * @return array         options, FALSE if not set
 */
function getoptVal($options, $short = null, $long = null, $default = false) {
	$val = array();

	if (isset($short) && isset($options[$short])) {
		if (is_array($options[$short]))
			$val = array_merge($val, $options[$short]);
		else
			$val[] = $options[$short];
	}
	elseif (isset($long) && isset($options[$long])) {
		if (is_array($options[$long]))
			$val = array_merge($val, $options[$long]);
		else
			$val[] = $options[$long];
	}

	return count($val) ? $val : $default;
}

if (php_sapi_name() == 'cli' || isset($_SERVER['SESSIONNAME']) && $_SERVER['SESSIONNAME'] == 'Console') {
	// parse options
	$options = getopt("u:t:hp", array('uuid:', 'total:', 'help', 'pretty'));

	$help    = getoptVal($options, 'h', 'help');
	$uuid    = getoptVal($options, 'u', 'uuid');
	$total   = getoptVal($options, 't', 'total');
	$pretty  = getoptVal($options, 'p', 'pretty');

	if ($help || empty($uuid)) {
		echo("Usage: settotal.php [options] value\n");
		echo("Options:\n");
		echo("             -u[uid] uuid\n");
		echo("            -t[otal] new total value\n");
		echo("Example:\n");
		echo("         settotal.php --uuid ABCD-0123 19\n");
		die;
	}

	$job = new SetTotal($uuid[0], $total[0], $pretty);
	$job->run();
}
