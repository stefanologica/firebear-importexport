<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Import;

use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingError;

/**
 * Class ProcessingErrorAggregator
 *
 * @package Firebear\ImportExport\Model\Import
 */
class ProcessingErrorAggregator extends \Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregator
{
    /**
     * @param string $errorLevel
     * @param int $count
     * @return ProcessingErrorAggregator
     */
    protected function setProcessErrorStatistics($errorLevel, $count)
    {
        $this->errorStatistics[$errorLevel] = $count;

        return $this;
    }

    public function hasToBeTerminated()
    {
        return $this->isErrorLimitExceeded();
    }

    public function isErrorLimitExceeded()
    {
        $isExceeded = false;
        $errorsCount = $this->getErrorsCount([ProcessingError::ERROR_LEVEL_CRITICAL]);
        if ($errorsCount > 0
            && $this->validationStrategy == self::VALIDATION_STRATEGY_STOP_ON_ERROR
            && $errorsCount > $this->allowedErrorsCount
        ) {
            $isExceeded = true;
        }

        return $isExceeded;
    }

    protected function isErrorAlreadyAdded($rowNum, $errorCode, $columnName = null)
    {
        return false;
    }
}
