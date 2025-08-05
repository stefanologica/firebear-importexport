<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Job;

use Firebear\ImportExport\Api\Data\ImportInterface;
use Firebear\ImportExport\Model\ResourceModel\Job\CollectionFactory;
use Magento\Framework\App\Request\DataPersistorInterface;
use Magento\Ui\DataProvider\Modifier\ModifierInterface;
use Magento\Ui\DataProvider\Modifier\PoolInterface;

/**
 * Class DataProvider
 */
class DataProvider extends \Magento\Ui\DataProvider\AbstractDataProvider
{

    /**
     * @var
     */
    protected $collection;

    /**
     * @var DataPersistorInterface
     */
    protected $dataPersistor;

    /**
     * @var array
     */
    protected $loadedData;

    /**
     * @var PoolInterface
     */
    protected $pool;

    /**
     * DataProvider constructor.
     * @param string $name
     * @param string $primaryFieldName
     * @param string $requestFieldName
     * @param CollectionFactory $importCollectionFactory
     * @param DataPersistorInterface $dataPersistor
     * @param PoolInterface $pool
     * @param array $meta
     * @param array $data
     */
    public function __construct(
        $name,
        $primaryFieldName,
        $requestFieldName,
        CollectionFactory $importCollectionFactory,
        DataPersistorInterface $dataPersistor,
        PoolInterface $pool,
        array $meta = [],
        array $data = []
    ) {
        $this->collection = $importCollectionFactory->create();
        $this->dataPersistor = $dataPersistor;
        parent::__construct($name, $primaryFieldName, $requestFieldName, $meta, $data);
        $this->pool = $pool;
    }

    /**
     * Get data
     *
     * @return array
     */
    public function getData()
    {
        if (isset($this->loadedData)) {
            return $this->loadedData;
        }
        $items = $this->collection->getItems();
        $mergeFields = [
            ImportInterface::BEHAVIOR_DATA,
            ImportInterface::SOURCE_DATA
        ];
        foreach ($items as $job) {
            $data = $job->getData();
            if ($maps = $job->getMap()) {
                $map = $this->scopeMaps($maps);
                $data = array_merge($data, $map);
                $data = array_merge($data, ['special_map' => $map]);
            }

            if (!empty($job->getMapping()) && $maps = \Zend\Serializer\Serializer::unserialize($job->getMapping())) {
                $map = $this->scopeCategoriesMapping($maps);
                $data = array_merge($data, $map);
                $data = array_merge($data, ['special_map_category' => $map]);

                $attributeValuesMap = $this->scopeAttributeValuesMapping($maps);
                $data = array_merge($data, $attributeValuesMap);
            }

            if (!empty($job->getPriceRules())
                && $priceRules = \Zend\Serializer\Serializer::unserialize($job->getPriceRules())
            ) {
                $priceRules = $this->scopePriceRules($priceRules);
                $data = array_merge($data, $priceRules);
            }

            foreach ($mergeFields as $name) {
                if ($data[$name]) {
                    $tempData = $data[$name];
                    unset($data[$name]);
                    $data += $tempData;
                }
            }
            $data = array_merge($data, $this->scopeVariations($data));
            $this->loadedData[$job->getId()] = $data;
        }

        $data = $this->dataPersistor->get('job');

        if (!empty($data)) {
            $job = $this->collection->getNewEmptyItem();
            $job->setData($data);
            $this->loadedData[$job->getId()] = $job->getData();
            $this->dataPersistor->clear('job');
        }

        return $this->loadedData;
    }

    /**
     * @return mixed
     */
    public function getMeta()
    {
        $meta = parent::getMeta();
        /** @var ModifierInterface $modifier */
        foreach ($this->pool->getModifiersInstances() as $modifier) {
            $meta = $modifier->modifyMeta($meta);
        }

        return $meta;
    }

    /**
     * @param $maps
     *
     * @return mixed
     */
    protected function scopeMaps($maps)
    {
        $map['source_data_map'] = [];
        $count = 0;
        foreach ($maps as $field) {
            $map['source_data_map'][] = [
                'source_data_system' => $field->getAttributeId()
                    ? $field->getAttributeId() : $field->getSpecialAttribute(),
                'source_data_import' => $field->getImportCode(),
                'source_data_replace' => $field->getDefaultValue(),
                'record_id' => $count++,
                'custom' => $field->getCustom()
            ];
        }

        return $map;
    }

    /**
     * Highlight attribute values data inside common maps
     *
     * @param array $maps
     * @return array
     */
    protected function scopeAttributeValuesMapping(array $maps)
    {
        $map['source_data_attribute_values_map'] = [];
        $count = 0;

        foreach ($maps as $field) {
            if (isset($field['source_data_attribute_value_system']) &&
                isset($field['source_data_attribute_value_import'])
            ) {
                $field['count'] = $count++;
                $map['source_data_attribute_values_map'][] = $field;
            }
        }

        return $map;
    }

    /**
     * @param $maps
     *
     * @return mixed
     */
    protected function scopeCategoriesMapping($maps)
    {
        $map['source_data_categories_map'] = [];
        $count = 0;
        foreach ($maps as $field) {
            if (isset($field['source_category_data_import']) && isset($field['source_category_data_new'])) {
                $map['source_data_categories_map'][] = [
                    'source_category_data_import' => $field['source_category_data_import'],
                    'source_category_data_new' => $field['source_category_data_new'],
                    'record_id' => $count++
                ];
            }
        }

        return $map;
    }

    /**
     * @param $priceRules
     *
     * @return mixed
     */
    protected function scopePriceRules($priceRules)
    {
        $result['price_rules_rows'] = [];
        $count = 0;
        foreach ($priceRules as $field) {
            if (isset($field['apply'], $field['value'], $field['price_rules_conditions_hidden'])) {
                $count++;
                $result['price_rules_rows'][] = [
                    'apply' => $field['apply'],
                    'value' => $field['value'],
                    'price_rules_conditions_hidden' => isset($field['price_rules_conditions_hidden'])
                        ? http_build_query($field['price_rules_conditions_hidden']) : '',
                    'record_id' => $count
                ];
            }
        }

        return $result;
    }

    /**
     * @param $maps
     * @return mixed
     */
    protected function scopeVariations($maps)
    {
        $map['configurable_variations'] = [];
        $count = 0;
        if (isset($maps['configurable_variations'])) {
            foreach ($maps['configurable_variations'] as $field) {
                $map['configurable_variations'][] = [
                    'configurable_variations_attributes' => $field,
                    'record_id' => $count++
                ];
            }
        }

        return $map;
    }
}
