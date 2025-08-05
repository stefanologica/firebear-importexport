<?php
/**
 * Fetchsheet
 *
 * @copyright Copyright © 2019 Firebear Studio. All rights reserved.
 * @author    fbeardev@gmail.com
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Exception;
use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job;
use Firebear\ImportExport\Helper\XlsxHelper;
use Firebear\ImportExport\Model\Job\Processor;
use Firebear\ImportExport\Model\Source\Type\AbstractType;
use Magento\Framework\App\Request\Http;
use Magento\Framework\App\ResponseInterface;
use Magento\Framework\Controller\Result\Json;
use Magento\Framework\Controller\ResultInterface;
use Magento\Framework\Exception\LocalizedException;

/**
 * Class Fetchsheet
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Fetchsheet extends Job
{
    /**
     * @var AbstractType
     */
    private $importSource;

    /**
     * @var XlsxHelper
     */
    private $xlsxHelper;

    /**
     * @var Processor
     */
    private $processor;

    /**
     * Fetchsheet constructor.
     *
     * @param Context $context
     * @param XlsxHelper $xlsxHelper
     * @param Processor $processor
     */
    public function __construct(
        Context $context,
        XlsxHelper $xlsxHelper,
        Processor $processor
    ) {
        $this->xlsxHelper = $xlsxHelper;
        parent::__construct($context);
        $this->processor = $processor;
    }

    /**
     * @return ResponseInterface|Json|ResultInterface
     */
    public function execute()
    {
        /** @var Json $resultJson */
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        $importData = $sheetsName = [];
        /** @var Http $request */
        $request = $this->getRequest();
        if ($request->isAjax()) {
            try {
                $formData = $this->getRequest()->getParam('form_data');
                $sourceType = $this->getRequest()->getParam('source_type');
                if (!empty($formData)) {
                    foreach ($formData as $data) {
                        $index = strstr($data, '+', true);
                        $index = str_replace($sourceType . '[', '', $index);
                        $index = str_replace(']', '', $index);
                        $importData[$index] = substr($data, strpos($data, '+') + 1);
                    }
                    if ($this->getRequest()->getParam('job_id')) {
                        $importData['job_id'] = (int)$this->getRequest()->getParam('job_id');
                    }
                    if (isset($importData['type_file'])) {
                        $this->processor->setTypeSource($importData['type_file']);
                    }
                    if (!in_array($importData['import_source'], ['rest', 'soap']) && isset($importData['file_path'])) {
                        $importData[$sourceType . '_file_path'] = $importData['file_path'];
                    }
                    if ($sourceType === 'file' && isset($importData['file_path'])) {
                        $file = $importData['file_path'];
                    } else {
                        $file = $this->getImportSource($importData)->uploadSource();
                    }

                    if ($file) {
                        $sheetsName = $this->xlsxHelper->getSheetsOptions($file);
                    }
                }
            } catch (Exception $exception) {
                $this->messageManager->addExceptionMessage($exception);
            }
        }
        return $resultJson->setData($sheetsName);
    }

    /**
     * @param array $sourceData
     * @return AbstractType
     * @throws LocalizedException
     */
    private function getImportSource(array $sourceData)
    {
        if ($this->importSource === null) {
            $this->importSource = $this->processor->getImportModel()
                ->setData($sourceData)
                ->getSource();
        }
        return $this->importSource;
    }
}
