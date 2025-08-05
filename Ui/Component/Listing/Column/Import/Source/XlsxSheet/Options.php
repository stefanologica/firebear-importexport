<?php
/**
 * Options
 *
 * @copyright Copyright © 2019 Firebear Studio. All rights reserved.
 * @author    fbeardev@gmail.com
 */

namespace Firebear\ImportExport\Ui\Component\Listing\Column\Import\Source\XlsxSheet;

use Firebear\ImportExport\Helper\XlsxHelper;
use Firebear\ImportExport\Model\Job\Processor;
use Firebear\ImportExport\Model\Source\Type\AbstractType;
use Magento\Framework\Data\OptionSourceInterface;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Registry;
use Magento\Framework\App\Helper\Context;

/**
 * Class Options
 *
 * @package Firebear\ImportExport\Ui\Component\Listing\Column\Import\Source\XlsxSheet
 */
class Options implements OptionSourceInterface
{
    /**
     * @var AbstractType
     */
    private $importSource;

    /**
     * Options array
     *
     * @var array
     */
    protected $options;

    /**
     * Core registry
     *
     * @var Registry
     */
    protected $coreRegistry;
    /**
     * @var XlsxHelper
     */
    private $xlsxHelper;

    /**
     * @var Processor
     */
    private $processor;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    protected $_logger;

    /**
     * Initialize options
     *
     * @param Registry $registry
     * @param XlsxHelper $xlsxHelper
     * @param Processor $processor
     * @param Context $context
     */
    public function __construct(
        Registry $registry,
        XlsxHelper $xlsxHelper,
        Processor $processor,
        Context $context
    ) {
        $this->coreRegistry = $registry;
        $this->xlsxHelper = $xlsxHelper;
        $this->processor = $processor;
        $this->_logger = $context->getLogger();
    }

    /**
     * Return array of options as value-label pairs
     *
     * @return array Format: array(array('value' => '<value>', 'label' => '<label>'), ...)
     * @throws LocalizedException
     */
    public function toOptionArray()
    {
        if (!$this->options) {
            $file = null;
            $model = $this->coreRegistry->registry('import_job');
            $options = [['label' => __('None'), 'value' => '']];
            if ($model && !empty($model->getData()) && !empty($model->getData('source_data'))) {
                $sourceData = $model->getData('source_data');
                $sourceType = $sourceData['import_source'] ?? 'file';
                if (!in_array($sourceData['import_source'], ['rest', 'soap']) && isset($sourceData['file_path'])) {
                    $sourceData[$sourceType . '_file_path'] = $sourceData['file_path'];
                }
                if ($sourceType === 'file') {
                    $file = $sourceData['file_path'] ?? '';
                } else {
                    $importSource = $this->getImportSource($sourceData);
                    if ($importSource) {
                        try {
                            $file = $importSource->uploadSource();
                        } catch (\Exception $e) {
                            $errorMessage = __($e->getMessage());
                            $this->_logger->critical($errorMessage);
                        }
                    }
                }
                if ($file) {
                    $options = $this->xlsxHelper->getSheetsOptions($file);
                }
            }
            $this->options = $options;
        }
        return $this->options ?: [];
    }

    /**
     * @param array $sourceData
     * @return AbstractType
     * @throws LocalizedException
     */
    private function getImportSource(array $sourceData)
    {
        if ($this->importSource === null
            && !empty($sourceData['import_source'])) {
            $this->importSource = $this->processor->getImportModel()
                ->setData($sourceData)
                ->getSource();
        }
        return $this->importSource;
    }
}
