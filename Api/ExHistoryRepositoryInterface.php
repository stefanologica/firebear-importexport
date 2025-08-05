<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Api;

use Firebear\ImportExport\Api\Data\ExportHistoryInterface;

/**
 * Interface JobRepositoryInterface
 *
 * @package Firebear\ExportExport\Api
 */
interface ExHistoryRepositoryInterface
{
    /**
     * Save job.
     *
     * @param ExportHistoryInterface $history
     * @return ExportHistoryInterface
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function save(ExportHistoryInterface $history);

    /**
     * Get history by id.
     *
     * @param int $id
     * @return ExportHistoryInterface
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function getById($id);

    /**
     * Delete history.
     *
     * @param ExportHistoryInterface $history
     * @return bool true on success
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function delete(ExportHistoryInterface $history);

    /**
     * Delete history by id.
     *
     * @param int $historyId
     * @return bool true on success
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function deleteById($historyId);
}
