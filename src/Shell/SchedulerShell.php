<?php
/**
 * SchedulerShell
 * Author: Trent Richardson [http://trentrichardson.com]
 *
 * Copyright 2015 Trent Richardson
 * You may use this project under MIT license.
 * http://trentrichardson.com/Impromptu/MIT-LICENSE.txt
 *
 * -------------------------------------------------------------------
 * To configure:
 * In your bootstrap_cli.php you must enable the plugin:
 *
 * Plugin::load('Scheduler');
 *
 * Then in bootstrap_cli.php schedule your jobs:
 *
 * Configure::write('SchedulerShell.jobs', [
 *  'CleanUp' => ['interval'=>'next day 5:00','task'=>'CleanUp'],// tomorrow at 5am
 *  'Newsletters' => ['interval'=>'PT15M','task'=>'Newsletter'] //every 15 minutes
 * ]);
 *
 * -------------------------------------------------------------------
 * Run a shell task:
 * - Cd into app dir
 * - run this:
 *  >> bin/cake Scheduler.Scheduler
 *
 * -------------------------------------------------------------------
 * Troubleshooting
 * - may have to run dos2unix to fix line endings in the bin/cake file
 * - if you didn't use composer to install this plugin you may need to
 *   enable the plugin with Plugin::load('Scheduler', [ 'autoload'=>true ]);
 */
namespace Scheduler\Shell;

use \DateInterval;
use \DateTime;
use Cake\Console\Shell;
use Cake\Core\Configure;
use Cake\Filesystem\File;
use Cake\Filesystem\Folder;

class SchedulerShell extends Shell
{

    /**
     * The array of scheduled tasks.
     */
    private $schedule = [];

    /**
     * The key which you set Configure::read() for your jobs
     */
    private $configKey = 'SchedulerShell';

    /**
     * The path where the store file is placed. null will store in Config folder
     */
    private $storePath = TMP;

    /**
     * The file name of the store
     */
    private $storeFile = 'cron_scheduler.json';

    /**
     * The file name of the processing flag file (indicates that existing job is running)
     */
    private $processingFlagFile = '.cron_scheduler_processing_flag';

    /**
     * The number of seconds to wait before running a parallel SchedulerShell
     */
    private $processingTimeout = 600;

    /**
     * The main method which you want to schedule for the most frequent interval
     *
     * @access public
     * @return void
     */
    public function main()
    {

        $this->init();

        // ok, run them when they're ready
        $this->__runJobs();
    }

    public function init()
    {
        $jobs = [];

        // read in the config
        if ($config = Configure::read($this->configKey)) {

            if (isset($config['storePath'])) {
                $this->storePath = $config['storePath'];
            }

            if (isset($config['storeFile'])) {
                $this->storeFile = $config['storeFile'];
            }

            if (isset($config['processingFlagFile'])) {
                $this->processingFlagFile = $config['processingFlagFile'];
            }

            if (isset($config['processingTimeout'])) {
                $this->processingTimeout = $config['processingTimeout'];
            }

            // read in the jobs from the config
            $jobs += $config['jobs'] ?? [];
        }

        // Read in the extra parameters schedule
        $extraSchedules = $this->param('schedule');
        if (is_array($extraSchedules)) {
            $jobs += $extraSchedules;
        }

        foreach ($jobs as $k => $v) {
            $v += ['extraParams' => []];
            $this->connect($k, $v['interval'], $v['command'], $v['extraParams']);
        }
    }

    /**
     * The connect method adds tasks to the schedule
     *
     * @access public
     * @param string $name - unique name for this job, isn't bound to anything and doesn't matter what it is
     * @param string $interval - date interval string "PT5M" (every 5 min) or a relative Date string "next day 10:00"
     * @param string $task - name of the cake task to call
     * @param string $action - name of the method within the task to call
     * @param array  $pass - array of arguments to pass to the method
     * @return void
     */
    public function connect($name, $interval, $command = [], $extraParams = [])
    {
        $this->schedule[$name] = [
            'name' => $name,
            'interval' => $interval,
            'command' => $command,
            'extraParams' => $extraParams,
            'lastRun' => null,
            'lastResult' => ''
        ];
    }

    /**
     * Process the tasks when they need to run
     *
     * @access private
     * @return void
     */
    private function __runJobs()
    {
        $dir = new Folder($this->storePath);

        // set processing flag so function takes place only once at any given time
        $processingFlag = new File($dir->slashTerm($dir->pwd()) . $this->processingFlagFile, false);
        $processing = $processingFlag->exists();

        if ($processing && (time() - $processingFlag->lastChange()) < $this->processingTimeout) {
            $this->out("Scheduler already running! Exiting.");
            return false;
        } else {
            $processingFlag->delete();
            $processingFlag->create();
        }

        // look for a store of the previous run
        $store = "";
        $storeFile = new File($dir->slashTerm($dir->pwd()) . $this->storeFile);
        if ($storeFile->exists()) {
            $store = $storeFile->read();
            $storeFile->close(); // just for safe measure
        }
        $this->out('Reading from: '. $storeFile->pwd());

        // build or rebuild the store
        if ($store != '') {
            $store = json_decode($store, true);
        } else {
            $store = $this->schedule;
        }

        // run the jobs that need to be run, record the time
        foreach ($this->schedule as $name => $job) {
            $now = new DateTime();
            $command = $job['command'];
            $extraParams = $job['extraParams'];

            // if the job has never been run before, create it
            if (!isset($store[$name])) {
                $store[$name] = $job;
            }

            // figure out the last run date
            $tmptime = $store[$name]['lastRun'];
            if ($tmptime == null) {
                $tmptime = new DateTime("1969-01-01 00:00:00");
            } elseif (is_array($tmptime)) {
                $tmptime = new DateTime($tmptime['date'], new DateTimeZone($tmptime['timezone']));
            } elseif (is_string($tmptime)) {
                $tmptime = new DateTime($tmptime);
            }

            // determine the next run time based on the last
            if (substr($job['interval'],0,1) === 'P') {
                $tmptime->add(new DateInterval($job['interval'])); // "P10DT4H" http://www.php.net/manual/en/class.dateinterval.php
            } else {
                $tmptime->modify($job['interval']); // "next day 10:30" http://www.php.net/manual/en/datetime.formats.relative.php
            }

            // is it time to run? has it never been run before?
            if ($tmptime <= $now) {
                $this->hr();
                $this->out("Running $name");
                $this->hr();

                // grab the entire schedule record incase it was updated..
                $store[$name] = $this->schedule[$name];

                // execute the command and store the result
                $store[$name]['lastResult'] = $this->dispatchShell([
                    'command' => $command,
                    'extra' => $extraParams
                ]);

                // assign it the current time
                $now = new DateTime();
                $store[$name]['lastRun'] = $now->format('Y-m-d H:i:s');
            } else {
                $this->hr();
                $this->out("Not time to run $name, skipping.");
                $this->hr();
            }
        }

        // write the store back to the file
        $storeFile->write(json_encode($store));
        $storeFile->close();

        // remove processing flag
        $processingFlag->delete();
    }
}
