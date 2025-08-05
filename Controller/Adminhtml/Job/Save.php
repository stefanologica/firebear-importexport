<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Api\Data\ImportInterface;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Firebear\ImportExport\Model\Job\MappingFactory;

/**
 * Class Save
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Save extends JobController
{
    /**
     * @var SerializerInterface
     */
    protected $serializer;

    /**
     * @var MappingFactory
     */
    protected $mappingFactory;

    /**
     * @var array
     */
    protected $additionalFields = [
        'platforms',
        'clear_attribute_value',
        'remove_product_association',
        'remove_product_website',
        'remove_all_customer_address',
        'remove_product_categories',
        'type_file',
        'configurable_switch',
        'configurable_create',
        'configurable_type',
        'configurable_field',
        'configurable_part',
        'configurable_symbols',
        'round_up_prices',
        'round_up_special_price',
        'copy_simple_value',
        'language',
        'reindex',
        'indexers',
        'generate_url',
        'enable_product_url_pattern',
        'product_url_pattern',
        'xml_switch',
        'root_category_id',
        'replace_default_value',
        'remove_current_mappings',
        'remove_images',
        'remove_images_dir',
        'remove_related_product',
        'remove_crosssell_product',
        'remove_upsell_product',
        'disable_products',
        'product_supplier',
        'send_email',
        'generate_shipment_by_track',
        'generate_invoice_by_track',
        'translate_attributes',
        'translate_store_ids',
        'translate_version',
        'translate_key',
        'translate_referer',
        'xlsx_sheet',
        'cron_groups',
        'email_type',
        'template',
        'receiver',
        'sender',
        'copy',
        'copy_method',
        'is_attach',
        'image_resize',
        'deferred_images',
        'cache_products',
        'increase_product_stock_by_qty'
    ];

    /**
     * Save constructor.
     *
     * @param Context $context
     * @param MappingFactory $mappingFactory
     */
    public function __construct(
        Context $context,
        MappingFactory $mappingFactory
    ) {
        parent::__construct($context);

        $this->mappingFactory = $mappingFactory;
    }

    /**
     * @return \Firebear\ImportExport\Controller\Adminhtml\Job\Save|\Magento\Backend\Model\View\Result\Redirect
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function execute()
    {
        /** @var \Magento\Backend\Model\View\Result\Redirect $resultRedirect */
        $resultRedirect = $this->resultRedirectFactory->create();
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        $data = $this->getRequest()->getPostValue();
        if ($data) {
            $jobId = $this->getRequest()->getParam('entity_id');
            if (isset($data['is_active']) && $data['is_active'] === 'true') {
                $data['is_active'] = ImportInterface::STATUS_ENABLED;
            }
            if (!$jobId) {
                $model = $this->jobFactory->create();
                $data['entity_id'] = null;
            } else {
                if (empty($data['entity_id'])) {
                    $data['entity_id'] = $jobId;
                }
                $model = $this->repository->getById($jobId);
                if (!$model->getId() && $jobId) {
                    $this->messageManager->addErrorMessage(__('This job no longer exists.'));

                    return $resultRedirect->setPath('*/*/');
                }
            }
            $data['import_source'] = $data['import_source'] ?? '';
            // Prepare Behavior Data
            $this->spliteBeahivorData($data);
            // Prepare Source Data
            $this->spliteSourceData($data);
            $model->addData($data);
            $dataMapWithDeleteData = [];
            if (isset($data['source_data_attribute_values_map'])) {
                $dataMapWithDeleteData = $data['source_data_attribute_values_map'];
            }
            if (isset($data['source_data_categories_map'])) {
                foreach ($data['source_data_categories_map'] as $categoryMapItems) {
                    if (!isset($categoryMapItems['delete'])) {
                        $dataMapWithDeleteData[] = $categoryMapItems;
                    }
                }
            }
            $model->setMapping(\Zend\Serializer\Serializer::serialize($dataMapWithDeleteData));

            $priceRules = [];
            if (isset($data['price_rules_rows'])) {
                foreach ($data['price_rules_rows'] as $priceRule) {
                    if (isset($priceRule['delete'])) {
                        continue;
                    }

                    if (isset($priceRule['price_rules_conditions_hidden'])) {
                        parse_str(
                            $priceRule['price_rules_conditions_hidden'],
                            $priceRule['price_rules_conditions_hidden']
                        );
                    }

                    $priceRules[] = $priceRule;
                }
            }
            $model->setPriceRules(\Zend\Serializer\Serializer::serialize($priceRules));

            // init model and set data
            $this->spliteMapsData($data, $model);

            // try to save it
            try {
                // save the data
                $newModel = $this->repository->save($model);
                // display success message
                if (!$this->getRequest()->isAjax()) {
                    $this->messageManager->addSuccessMessage(__('Job was saved successfully.'));
                }
                // clear previously saved data from session
                $this->_session->setFormData(false);
                // check if 'Save and Continue'
                if ($this->getRequest()->isAjax()) {
                    return $resultJson->setData($newModel->getId());
                }
                if ($this->getRequest()->getParam('back')) {
                    return $resultRedirect->setPath('*/*/edit', ['entity_id' => $model->getId()]);
                }

                // go to grid
                return $resultRedirect->setPath('*/*/');
            } catch (\Exception $e) {
                // display error message
                $this->messageManager->addErrorMessage($e->getMessage());
                $this->_session->setFormData($data);
                if ($this->getRequest()->isAjax()) {
                    return $resultJson->setData(false);
                }
                // redirect to edit form
                return $resultRedirect->setPath(
                    '*/*/edit',
                    ['entity_id' => $this->getRequest()->getParam('entity_id')]
                );
            }
        }
        if ($this->getRequest()->isAjax()) {
            return $resultJson->setData(true);
        }

        return $resultRedirect->setPath('*/*/');
    }

    /**
     * @param $data
     */
    protected function spliteBeahivorData(&$data)
    {
        $behavior = $data['behavior'];
        $jobModel = $this->jobFactory->create();
        $behaviorFields = $jobModel->getBehaviorFormFields();
        $data['behavior_data']['behavior'] = $behavior;
        foreach ($behaviorFields as $field) {
            if (!isset($data[$field])) {
                continue;
            }
            $data['behavior_data'][$field] = $data[$field];
            unset($data[$field]);
        }
        unset($data['behavior']);
    }

    /**
     * @param $data
     */
    protected function spliteSourceData(&$data)
    {
        $fields = $this->helper->getConfigFields();
        $data['source_data']['import_source'] = $data['import_source'];
        foreach ($fields as $field) {
            if (isset($data[$field])) {
                $data['source_data'][$field] = $data[$field];
                unset($data[$field]);
            }
        }

        foreach ($this->additionalFields as $adField) {
            if (isset($data[$adField])) {
                $data['source_data'][$adField] = $data[$adField];
                unset($data[$adField]);
            }
        }
        if (isset($data['configurable_variations'])) {
            $confData = [];
            foreach ($data['configurable_variations'] as $row) {
                if (!(isset($row['delete']) && $row['delete'] == true)) {
                    $confData[] = $row['configurable_variations_attributes'];
                }
            }
            $data['source_data']['configurable_variations'] = $confData;
        }

        if (isset($data['copy_simple_value'])) {
            $confData = [];
            foreach ($data['copy_simple_value'] as $row) {
                if (!(isset($row['delete']) && $row['delete'] == true)) {
                    $confData[] = $row['copy_simple_value_attributes'];
                }
            }
            $data['source_data']['copy_simple_value'] = $confData;
        }
    }

    /**
     * @param $data
     * @param $model
     */
    protected function spliteMapsData(&$data, $model)
    {
        $model->deleteMap();
        if (isset($data['source_data_map'])) {
            foreach ($data['source_data_map'] as $row) {
                if (!(isset($row['delete']) && $row['delete'] == true)) {
                    $newMap = $this->mappingFactory->create();
                    $newMap->setImportCode($row['source_data_import']);
                    if (is_numeric($row['source_data_system'])) {
                        $newMap->setAttributeId((int)$row['source_data_system']);
                    } else {
                        $newMap->setSpecialAttribute($row['source_data_system']);
                    }
                    if (isset($row['source_data_replace']) && $row['source_data_replace'] != '') {
                        $newMap->setDefaultValue($row['source_data_replace']);
                    }
                    $newMap->setCustom($row['custom']);
                    $model->addMap($newMap);
                }
            }
            unset($data['source_data_map']);
        }
    }
}
