<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Export;

use Firebear\ImportExport\Traits\Export\Entity as ExportTrait;
use Magento\CustomerImportExport\Model\Export\Address as MagentoAddress;
use Magento\Eav\Model\Entity\Collection\AbstractCollection;
use Magento\Framework\Exception\LocalizedException;

/**
 * Class Address
 *
 * @package Firebear\ImportExport\Model\Export
 */
class Address extends MagentoAddress implements EntityInterface
{
    use ExportTrait;

    /**
     * @inheritdoc
     */
    protected function _initCustomers()
    {
        return $this;
    }

    /**
     * Retrieve entity field for export
     *
     * @return array
     */
    public function getFieldsForExport()
    {
        return array_unique(
            array_merge(
                $this->_permanentAttributes,
                $this->_getExportAttributeCodes(),
                array_keys(self::$_defaultAddressAttributeMapping)
            )
        );
    }

    /**
     * Get additional customer attributes
     *
     * @return array
     */
    protected function _getExportAttributeCodes()
    {
        parent::_getExportAttributeCodes();

        $customerAttributes = [
            self::COLUMN_WEBSITE,
            self::COLUMN_EMAIL,
            self::COLUMN_NAME_DEFAULT_BILLING,
            self::COLUMN_NAME_DEFAULT_SHIPPING
        ];

        foreach ($customerAttributes as $attribute) {
            if (!in_array($attribute, $this->_attributeCodes)) {
                $this->_attributeCodes[] = $attribute;
            }
        }

        return $this->_attributeCodes;
    }

    /**
     * @param $item
     * @throws LocalizedException
     */
    public function exportItem($item)
    {
        $row = $this->_addAttributeValuesToRow($item);

        foreach (self::$_defaultAddressAttributeMapping as $columnName => $attributeCode) {
            if (!empty($row[$columnName]) && $row[$columnName] == $item->getId()) {
                $row[$columnName] = 1;
            }
        }
        if ($this->_parameters['enable_last_entity_id'] > 0) {
            $this->lastEntityId = $item['entity_id'];
        }

        $row[self::COLUMN_ADDRESS_ID] = $item['entity_id'];
        if (isset($this->_websiteIdToCode[$item[self::COLUMN_WEBSITE]])) {
            $row[self::COLUMN_WEBSITE] = $this->_websiteIdToCode[$item[self::COLUMN_WEBSITE]];
        }

        $this->getWriter()->writeRow($this->changeRow($row));
    }

    /**
     * Export process
     *
     * @return array
     * @throws LocalizedException
     */
    public function export()
    {
        // skip and filter by customer address attributes
        $entityCollection = $this->_getEntityCollection();
        if (isset($this->_parameters['last_entity_id'])
            && $this->_parameters['last_entity_id'] > 0
            && $this->_parameters['enable_last_entity_id'] > 0
        ) {
            $entityCollection->addFieldToFilter(
                'entity_id',
                ['gt' => $this->_parameters['last_entity_id']]
            );
        }
        $this->_prepareEntityCollection($entityCollection);

        // prepare headers
        $this->getWriter()->setHeaderCols($this->_getHeaderColumns());

        $this->_exportCollectionByPages($entityCollection);

        return [$this->getWriter()->getContents(), $entityCollection->getSize(), $this->lastEntityId];
    }

    /**
     * Retrieve header columns
     *
     * @return array
     */
    protected function _getHeaderColumns()
    {
        $headers = array_merge(
            $this->_permanentAttributes,
            $this->_getExportAttributeCodes(),
            array_keys(self::$_defaultAddressAttributeMapping)
        );

        return $this->changeHeaders($headers);
    }

    /**
     * Apply filter to collection and add not skipped attributes to select
     *
     * @param AbstractCollection $collection
     * @return AbstractCollection
     */
    protected function _prepareEntityCollection(AbstractCollection $collection)
    {
        $this->filterEntityCollection($collection);
        $this->_addAttributesToCollection($collection);
        $collection->getSelect()
            ->join(
                ['ce' => $collection->getTable('customer_entity')],
                'e.parent_id = ce.entity_id',
                [
                    self::COLUMN_WEBSITE => 'website_id',
                    self::COLUMN_EMAIL => 'email',
                    self::COLUMN_NAME_DEFAULT_BILLING => 'default_billing',
                    self::COLUMN_NAME_DEFAULT_SHIPPING => 'default_shipping']
            );
        return $collection;
    }

    /**
     * Retrieve attributes codes which are appropriate for export
     *
     * @return array
     */
    protected function _getExportAttrCodes()
    {
        return [];
    }
}
