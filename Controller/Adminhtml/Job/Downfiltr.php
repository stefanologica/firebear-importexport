<?php
/**
 * @copyright: Copyright © 2018 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Firebear\ImportExport\Model\Export\Customer\Additional as CustomerAdditional;
use Firebear\ImportExport\Model\Export\EntityInterface;
use Firebear\ImportExport\Model\Export\Dependencies\Config as ExportConfig;
use Firebear\ImportExport\Model\Export\Product\Additional as ProductAdditional;
use Firebear\ImportExport\Model\Source\Factory as ModelFactory;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\CollectionFactory as AttributeCollectionFactory;
use Magento\Eav\Model\ResourceModel\Entity\Type\CollectionFactory as TypeCollectionFactory;

/**
 * Class Downfiltr
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Downfiltr extends JobController
{
    /**
     * @var AttributeCollectionFactory
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
     * @var TypeCollectionFactory
     */
    protected $typeCollection;

    /**
     * @var ProductAdditional
     */
    protected $additional;

    /**
     * @var CustomerAdditional
     */
    protected $additionalCust;

    /**
     * @var array
     */
    protected $customersArray = [
        'customer'
    ];

    /**
     * Downfiltr constructor.
     *
     * @param Context $context
     * @param AttributeCollectionFactory $collectionFactory
     * @param ExportConfig $config
     * @param ModelFactory $createFactory
     * @param TypeCollectionFactory $typeCollection
     * @param ProductAdditional $additional
     * @param CustomerAdditional $additionalCust
     */
    public function __construct(
        Context $context,
        AttributeCollectionFactory $collectionFactory,
        ExportConfig $config,
        ModelFactory $createFactory,
        TypeCollectionFactory $typeCollection,
        ProductAdditional $additional,
        CustomerAdditional $additionalCust
    ) {
        parent::__construct($context);

        $this->collection = $collectionFactory;
        $this->config = $config;
        $this->createFactory = $createFactory;
        $this->typeCollection = $typeCollection;
        $this->additional = $additional;
        $this->additionalCust = $additionalCust;
    }

    /**
     * @return \Magento\Framework\Controller\ResultInterface
     */
    public function execute()
    {
        /** @var \Magento\Framework\Controller\Result\Json $resultJson */
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        $options = [];
        $tables = $this->getFromTables();
        $extTypes = array_keys($tables);
        if ($this->getRequest()->isAjax()) {
            $entity = $this->getRequest()->getParam('entity');
            if ($entity) {
                if (!in_array($entity, $extTypes) || 'advanced_pricing' == $entity) {
                    $list = $this->getFromAttributes($entity);
                    if (in_array($entity, ['advanced_pricing', 'catalog_product'])) {
                        foreach ($this->uniqualFields() as $field) {
                            $list[] = $field;
                        }
                    }
                    if ('advanced_pricing' == $entity) {
                        $list = array_merge_recursive($list, $tables[$entity]);
                    }
                    if (in_array($entity, $this->customersArray)) {
                        foreach ($this->uniqualFieldsCust() as $field) {
                            $list[] = $field;
                        }
                    }
                    $options = $list;
                } else {
                    $options = $tables[$entity];
                }
            }
        }

        return $resultJson->setData($options);
    }

    /**
     * @param $type
     * @return array
     */
    protected function getFromAttributes($type)
    {
        $options = [];
        if ($type == 'advanced_pricing') {
            $type = 'catalog_product';
        }
        $types = $this->typeCollection->create()->addFieldToFilter('entity_type_code', $type);
        if ($types->getSize()) {
            $collection = $this->collection->create()->addFieldToFilter(
                'entity_type_id',
                $types->getFirstItem()->getId()
            );
            foreach ($collection as $item) {
                $options[] = [
                    'value' => $item->getAttributeCode(),
                    'label' => $item->getFrontendLabel() ? $item->getFrontendLabel() : $item->getAttributeCode()
                ];
            }
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
            $typeModel = $type['model'];
            /** @var EntityInterface $model */
            $model = $this->createFactory->create($typeModel);
            $options += $model->getFieldsForFilter();
        }

        return $options;
    }

    /**
     * @return array
     */
    protected function uniqualFields()
    {
        return $this->additional->toOptionArray();
    }

    /**
     * @return array
     */
    protected function uniqualFieldsCust()
    {
        return $this->additionalCust->toOptionArray();
    }
}
