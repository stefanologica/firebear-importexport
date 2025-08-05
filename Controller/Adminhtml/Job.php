<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml;

use Firebear\ImportExport\Api\JobRepositoryInterface;
use Firebear\ImportExport\Model\JobFactory;
use Firebear\ImportExport\Helper\Data as Helper;
use Magento\Backend\App\Action;

/**
 * Class Job
 *
 * @package Firebear\ImportExport\Controller\Adminhtml
 */
abstract class Job extends Action
{
    const ADMIN_RESOURCE = 'Firebear_ImportExport::job';

    /**
     * @var JobFactory
     */
    protected $jobFactory;

    /**
     * @var JobRepositoryInterface
     */
    protected $repository;

    /**
     * @var Helper
     */
    protected $helper;

    /**
     * Job constructor.
     *
     * @param Context $context
     */
    public function __construct(
        Context $context
    ) {
        parent::__construct(
            $context->getContext()
        );

        $this->jobFactory = $context->getJobFactory();
        $this->repository = $context->getRepository();
        $this->helper = $context->getHelper();
    }
}
