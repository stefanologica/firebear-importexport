<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio GmbH. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model;

use Firebear\ImportExport\Api\Data\ImportInterface;
use Firebear\ImportExport\Api\JobRepositoryInterface;
use Firebear\ImportExport\Model\ResourceModel\Job as ImportJobResource;
use Firebear\ImportExport\Model\ResourceModel\Job\CollectionFactory as ImportCollectionFactory;
use Magento\Framework\Exception\CouldNotDeleteException;
use Magento\Framework\Exception\CouldNotSaveException;
use Magento\Framework\Exception\NoSuchEntityException;

/**
 * Class JobRepository
 *
 * @package Firebear\ImportExport\Model
 */
class JobRepository implements JobRepositoryInterface
{

    /**
     * @var ImportJobResource
     */
    protected $resource;

    /**
     * @var JobFactory
     */
    protected $importFactory;

    /**
     * @var ImportCollectionFactory
     */
    protected $importCollectionFactory;

    /**
     * @var \Firebear\ImportExport\Model\JobRegistry
     */
    protected $jobRegistry;

    /**
     * JobRepository constructor.
     * @param ImportJobResource $resource
     * @param JobRegistry $jobRegistry
     * @param JobFactory $importFactory
     * @param ImportCollectionFactory $importCollectionFactory
     */
    public function __construct(
        ImportJobResource $resource,
        \Firebear\ImportExport\Model\JobRegistry $jobRegistry,
        \Firebear\ImportExport\Model\JobFactory $importFactory,
        ImportCollectionFactory $importCollectionFactory
    ) {
        $this->resource = $resource;
        $this->jobRegistry = $jobRegistry;
        $this->importFactory = $importFactory;
        $this->importCollectionFactory = $importCollectionFactory;
    }

    /**
     * {@inheritdoc}
     */
    public function save(ImportInterface $job)
    {
        try {
            $this->resource->save($job);
        } catch (\Exception $exception) {
            throw new CouldNotSaveException(__(
                'Could not save the job: %1',
                $exception->getMessage()
            ));
        }

        return $job;
    }

    /**
     * {@inheritdoc}
     */
    public function getById($jobId)
    {
        $job = $this->importFactory->create();
        $this->resource->load($job, $jobId);

        if (!$job->getId()) {
            throw new NoSuchEntityException(__('Job with id "%1" does not exist.', $jobId));
        }

        return $job;
    }

    /**
     * {@inheritdoc}
     */
    public function delete(ImportInterface $job)
    {
        try {
            $this->resource->delete($job);
        } catch (\Exception $exception) {
            throw new CouldNotDeleteException(__(
                'Could not delete the job: %1',
                $exception->getMessage()
            ));
        }
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteById($jobId)
    {
        return $this->delete($this->getById($jobId));
    }
}
