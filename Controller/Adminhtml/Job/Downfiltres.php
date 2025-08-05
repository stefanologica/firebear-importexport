<?php
/**
 * @copyright: Copyright © 2018 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Firebear\ImportExport\Model\Export\Customer\Additional as CustomerAdditional;
use Firebear\ImportExport\Model\Export\Dependencies\Config as ExportConfig;
use Firebear\ImportExport\Model\Export\EntityInterface;
use Firebear\ImportExport\Model\Export\Product\Additional as ProductAdditional;
use Firebear\ImportExport\Model\Source\Factory as ModelFactory;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\Collection as AttributeCollection;
use Magento\Eav\Model\Entity\TypeFactory as TypeFactory;

/**
 * Class Downfiltres
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Downfiltres extends JobController
{
    /**
     * @var AttributeCollection
     */
    protected $collection;

    /**
     * @var ExportConfig
     */
    protected $config;

    /**
     * @var ModelFactory
     */
    protected $createFactory;

    /**
     * @var ProductAdditional
     */
    protected $additional;

    /**
     * @var CustomerAdditional
     */
    protected $additionalCust;

    /**
     * @var TypeFactory
     */
    protected $eavTypeFactory;

    /**
     * Downfiltres constructor.
     *
     * @param Context $context
     * @param AttributeCollection $collection
     * @param ExportConfig $config
     * @param ModelFactory $createFactory
     * @param ProductAdditional $additional
     * @param CustomerAdditional $additionalCust
     * @param TypeFactory $eavTypeFactory
     */
    public function __construct(
        Context $context,
        AttributeCollection $collection,
        ExportConfig $config,
        ModelFactory $createFactory,
        ProductAdditional $additional,
        CustomerAdditional $additionalCust,
        TypeFactory $eavTypeFactory
    ) {
        parent::__construct($context);

        $this->collection = $collection;
        $this->config = $config;
        $this->createFactory = $createFactory;
        $this->additional = $additional;
        $this->additionalCust = $additionalCust;
        $this->eavTypeFactory = $eavTypeFactory;
    }

    /**
     * @return \Magento\Framework\Controller\ResultInterface
     */
    public function execute()
    {
        /** @var \Magento\Framework\Controller\Result\Json $resultJson */
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        $options = [];
        $result = [];
        if ($this->getRequest()->isAjax()) {
            $attribute = $this->getRequest()->getParam('attribute');
            $type = $this->getRequest()->getParam('type');
            if ($attribute && $type) {
                $options = array_merge_recursive(
                    $this->getFromAttributes(),
                    $this->getFromTables()
                );
            }
            if (!empty($options)) {
                foreach ($options[$type] as $field) {
                    if ($attribute == $field['field']) {
                        $result = $field;
                    }
                }
            }
            return $resultJson->setData($result);
        }
    }

    /**
     * @return array
     */
    protected function getFromAttributes()
    {
        $options = [];
        $options['attr'] = [];
        $entity = $this->getRequest()->getParam('entity');
        $entityType = $this->eavTypeFactory->create()->loadByCode($entity);
        $attribute = $this->getRequest()->getParam('attribute');
        $collection = $this->collection->addFieldToFilter('attribute_code', $attribute);
        $collection->setEntityTypeFilter($entityType->getId());

        foreach ($collection as $item) {
            $select = [];
            $type = $item->getFrontendInput();
            if (in_array($type, [\Magento\ImportExport\Model\Export::FILTER_TYPE_SELECT, 'multiselect'])) {
                if ($optionsAttr = $item->getSource()->getAllOptions()) {
                    foreach ($optionsAttr as $option) {
                        if (isset($option['value'])) {
                            $select[] = ['label' => $option['label'], 'value' => $option['value']];
                        }
                    }
                }
            }

            if ($item->getFrontendInput() != 'select'
                && in_array($item->getBackendType(), ['int', 'decimal'])) {
                $type = 'int';
            }
            if (in_array($item->getFrontendInput(), ['textarea', 'media_image', 'image', 'multiline', 'gallery'])) {
                $type = 'text';
            }
            if (in_array($item->getFrontendInput(), ['hidden'])) {
                $type = 'not';
            }
            if (in_array($item->getFrontendInput(), ['multiselect'])) {
                $type = 'select';
            }
            if ($item->getFrontendInput() == 'boolean') {
                $type = 'select';
                $select[] = ['label' => __('Yes'), 'value' => 1];
                $select[] = ['label' => __('No'), 'value' => 0];
            }
            if ($item->getAttributeCode() == 'category_ids') {
                $type = 'int';
            }

            $options['attr'][] =
                [
                    'field' => $item->getAttributeCode(),
                    'type' => $type,
                    'select' => $select
                ];
        }
        foreach ($this->additional->getAdditionalFields() as $field) {
            $options['attr'][] = $field;
        }
        foreach ($this->additionalCust->getAdditionalFields() as $field) {
            $options['attr'][] = $field;
        }
        return $options;
    }

    /**
     * @return array
     */
    protected function getFromTables()
    {
        $options = [];
        $data = $this->config->get();
        foreach ($data as $typeName => $type) {
            /** @var EntityInterface $model */
            $model = $this->createFactory->create($type['model']);
            $columns = $model->getFieldColumns();

            if ('advanced_pricing' == $typeName) {
                if (empty($options['attr'])) {
                    $options['attr'] = [];
                }
                $options['attr'] += $columns['advanced_pricing'];
            } else {
                $options += $columns;
            }
        }
        return $options;
    }
}
