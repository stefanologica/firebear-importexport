<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Api;

use Firebear\ImportExport\Api\Data\ImportInterface;

/**
 * Interface JobRepositoryInterface
 *
 * @package Firebear\ImportExport\Api
 */
interface JobRepositoryInterface
{
    /**
     * Save job.
     *
     * @param ImportInterface $job
     * @return ImportInterface
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function save(ImportInterface $job);

    /**
     * Get job by id.
     *
     * @param int $jobId
     * @return ImportInterface
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function getById($jobId);

    /**
     * Delete job.
     *
     * @param ImportInterface $job
     * @return bool true on success
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function delete(ImportInterface $job);

    /**
     * Delete job by id.
     *
     * @param int $jobId
     * @return bool true on success
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function deleteById($jobId);
}
