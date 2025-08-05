<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Console\Command;

use Firebear\ImportExport\Api\JobRepositoryInterface;
use Firebear\ImportExport\Model\Email\Sender;
use Firebear\ImportExport\Model\JobFactory;
use Firebear\ImportExport\Model\Job\Processor;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Output\OutputInterface;
use Psr\Log\LoggerInterface;

/**
 * Command prints list of available currencies
 */
class ImportJobAbstractCommand extends Command
{
    const JOB_ARGUMENT_NAME = 'job';

    /**
     * @var JobFactory
     */
    protected $factory;

    /**
     * @var JobRepositoryInterface
     */
    protected $repository;

    /**
     * @var Processor
     */
    protected $processor;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    protected $debugMode;

    /**
     * @var \Firebear\ImportExport\Helper\Data
     */
    protected $helper;

    /**
     * @var \Magento\Framework\App\State
     */
    protected $state;

    protected $loggerRun;

    /**
     * Email sender
     *
     * @var Sender
     */
    protected $sender;

    /**
     * ImportJobAbstractCommand constructor.
     *
     * @param JobFactory $factory
     * @param JobRepositoryInterface $repository
     * @param \Firebear\ImportExport\Logger\Logger $logger
     * @param Processor $importProcessor
     * @param \Firebear\ImportExport\Helper\Data $helper
     * @param \Magento\Framework\App\State $state
     * @param Sender $sender
     */
    public function __construct(
        JobFactory $factory,
        JobRepositoryInterface $repository,
        \Firebear\ImportExport\Logger\Logger $logger,
        Processor $importProcessor,
        \Firebear\ImportExport\Helper\Data $helper,
        \Magento\Framework\App\State $state,
        Sender $sender
    ) {
        $this->factory = $factory;
        $this->repository = $repository;
        $this->processor = $importProcessor;
        $this->state = $state;
        $this->logger = $logger;
        $this->helper = $helper;
        $this->sender = $sender;
        parent::__construct();
    }

    /**
     * @param $debugData
     * @param OutputInterface|null $output
     * @param null $type
     * @return $this
     */
    public function addLogComment($debugData, OutputInterface $output = null, $type = null)
    {

        if ($this->debugMode) {
            $this->logger->debug($debugData);
        }

        if ($output) {
            switch ($type) {
                case 'error':
                    $debugData = '<error>' . $debugData . '</error>';
                    break;
                case 'info':
                    $debugData = '<info>' . $debugData . '</info>';
                    break;
                default:
                    $debugData = '<comment>' . $debugData . '</comment>';
                    break;
            }

            $output->writeln($debugData);
        }

        return $this;
    }

    /**
     * @param $logger
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
    }
}
