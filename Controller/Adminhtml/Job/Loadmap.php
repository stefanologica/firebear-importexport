<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Firebear\ImportExport\Model\Import\Platforms;
use Firebear\ImportExport\Model\Job\Processor;
use Firebear\ImportExport\Model\Source\Config\CartPrice;
use Firebear\ImportExport\Ui\Component\Listing\Column\Entity\Import\Options;

/**
 * Class Loadmap
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Loadmap extends JobController
{
    /**
     * @var Processor
     */
    protected $processor;

    /**
     * @var Platforms
     */
    protected $platforms;

    /**
     * @var Options
     */
    protected $options;

    /**
     * @var CartPrice
     */
    protected $cartPrice;

    /**
     * Loadmap constructor.
     *
     * @param Context $context
     * @param Platforms $platforms
     * @param Processor $processor
     * @param Options $options
     * @param CartPrice $cartPrice
     */
    public function __construct(
        Context $context,
        Platforms $platforms,
        Processor $processor,
        Options $options,
        CartPrice $cartPrice
    ) {
        parent::__construct($context);

        $this->platforms = $platforms;
        $this->processor = $processor;
        $this->options = $options;
        $this->cartPrice = $cartPrice;
    }

    /**
     * @return \Magento\Framework\Controller\ResultInterface
     */
    public function execute()
    {
        /** @var \Magento\Framework\Controller\Result\Json $resultJson */
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        if ($this->getRequest()->isAjax()) {
            //read required fields from xml file
            $type = $this->getRequest()->getParam('type');
            $locale = $this->getRequest()->getParam('language');
            $importData = [];
            $importData['platforms'] = $type;
            $importData['locale'] = $locale;
            $importData['import_source'] = $importData['import_source'] ?? '';
            $maps = [];
            if ($type) {
                $mapArr = $this->platforms->getAllData($type);
                if (!empty($mapArr)) {
                    $maps = $mapArr;
                }
            }
            //get CSV Columns from CSV Import file
            $formData = $this->getRequest()->getParam('form_data');
            $sourceType = $this->getRequest()->getParam('source_type');
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

            try {
                $result = $this->processor->getColumns($importData);
                //load categories map
                foreach ($result as $key => $el) {
                    if (preg_match('/^(attribute\|).+/', $el)) {
                        unset($result[$key]);
                    }
                }

                if (is_array($result)) {
                    $messages = [];
                }
            } catch (\Exception $e) {
                return $resultJson->setData(['error' => $e->getMessage()]);
            }
            /*render Import Attribute dropdown*/
            if (!is_array($result)) {
                return $resultJson->setData(['error' => $result]);
            }
            $options = [];
            if ($importData['entity']) {
                /* Validating the csv file header for CART PRICE RULE */
                if ($importData['entity'] == 'cart_price_rule') {
                    $custom_attribute = 'cart_price_rule';
                    $cartPriceData = $this->cartPrice->toArray();
                    foreach ($cartPriceData as $key => $value) {
                        if (in_array($value, $result)) {
                            $ruleMessages = 'Success';
                        } else {
                            if ($value == 'coupon_code') {
                                if (in_array('code', $result)) {
                                    $ruleMessages = 'Success';
                                }
                            } else {
                                $ruleMessages = 'Error';
                                break;
                            }
                        }
                    }
                    return $resultJson->setData(
                        [
                            'map' => $maps,
                            'columns' => $result,
                            'messages' => $messages,
                            'options' => $options,
                            'show' => $custom_attribute,
                            'ruleMessages' => $ruleMessages
                        ]
                    );
                } else {
                    $collect = $this->options->toOptionArray(1, $importData['entity']);
                    $options = $collect[$importData['entity']];
                    return $resultJson->setData(
                        [
                            'map' => $maps,
                            'columns' => $result,
                            'messages' => $messages,
                            'options' => $options
                        ]
                    );
                }
            }
        }
    }
}
