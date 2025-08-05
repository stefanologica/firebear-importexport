<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Source\Platform;

use Firebear\ImportExport\Model\Import\Product;
use Magento\Backend\Model\Session;
use Magento\Catalog\Model\Product\Visibility;
use Magento\Catalog\Model\ResourceModel\Eav\Attribute;
use Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModelFactory;
use Magento\Eav\Model\Entity\Context;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\Set\CollectionFactory;
use Magento\Eav\Setup\EavSetupFactory;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\DataObject;
use Magento\Framework\File\Csv;
use Magento\Framework\Filesystem;
use Magento\Framework\Filesystem\File\ReadFactory;
use Magento\Store\Model\StoreManagerInterface;
use Magento\Tax\Model\ClassModelFactory;

/**
 * Abstract class for import source types
 * @method \Magento\ImportExport\Model\Import\AbstractSource getSource(array $data)
 *
 * @package Firebear\ImportExport\Model\Source\Platform
 */
abstract class AbstractPlatform extends DataObject implements PlatformInterface
{

    /**
     * CSV Processor
     *
     * @var Csv
     */
    protected $csvProcessor;

    /**
     * @var ScopeConfigInterface
     */
    protected $scopeConfig;

    /**
     * @var \Magento\Framework\Filesystem\Directory\WriteInterface
     */
    protected $directory;

    /**
     * @var Filesystem
     */
    protected $filesystem;

    /**
     * @var ReadFactory
     */
    protected $readFactory;

    /**
     * @var ClassModelFactory
     */
    protected $taxFactory;

    /**
     * @var Visibility
     */
    protected $visibility;

    /**
     * @var CollectionFactory
     */
    protected $attributeSetCollectionFactory;

    /**
     * @var Product
     */
    protected $importProduct;

    /**
     * @var Config
     */
    protected $eavConfig;

    /**
     * @var EavSetupFactory
     */
    protected $eavSetupFactory;

    /**
     * @var StoreManagerInterface
     */
    protected $storeManager;

    /**
     * @var Attribute
     */
    protected $attributeFactory;

    /**
     * @var ResourceModelFactory
     */
    protected $resourceFactory;

    /**
     * DB connection.
     *
     * @var \Magento\Framework\DB\Adapter\AdapterInterface
     */
    protected $connection;

    /**
     * @var Session
     */
    protected $session;

    /**
     * @var array
     */
    protected $unsetColumns;

    public function __construct(
        ScopeConfigInterface $scopeConfig,
        Filesystem $filesystem,
        ReadFactory $readFactory,
        Csv $csvProcessor,
        ClassModelFactory $taxFactory,
        Visibility $visibility,
        CollectionFactory $attributeSetCollectionFactory,
        Product $importProduct,
        Context $context,
        EavSetupFactory $eavSetupFactory,
        StoreManagerInterface $storeManager,
        Attribute $attributeFactory,
        ResourceModelFactory $resourceFactory,
        Session $session,
        array $data = []
    ) {
        $this->scopeConfig                   = $scopeConfig;
        $this->filesystem                    = $filesystem;
        $this->directory                     = $filesystem->getDirectoryWrite(DirectoryList::ROOT);
        $this->readFactory                   = $readFactory;
        $this->csvProcessor                  = $csvProcessor;
        $this->taxFactory                    = $taxFactory;
        $this->visibility                    = $visibility;
        $this->attributeSetCollectionFactory = $attributeSetCollectionFactory;
        $this->importProduct                 = $importProduct;
        $this->eavConfig                     = $context->getEavConfig();
        $this->eavSetupFactory               = $eavSetupFactory;
        $this->storeManager                  = $storeManager;
        $this->attributeFactory              = $attributeFactory;
        $this->resourceFactory               = $resourceFactory;
        $this->connection                    = $context->getResource()->getConnection();
        $this->session                       = $session;
        $this->unsetColumns                  = [
            '_root_category',
            '_category',
            '_type',
            '_links_related_sku',
            '_links_crosssell_sku',
            '_links_upsell_sku',
            '_links_related_position',
            '_links_crosssell_position',
            '_links_upsell_position',
            '_group_price_website',
            '_group_price_customer_group',
            '_group_price_price',
            '_media_lable',
            'tax_class_id',
            '_store',
            '_super_attribute_price_corr',
            '_super_products_sku',
            '_super_attribute_code',
            '_super_attribute_option',
            '_custom_option_store',
            '_custom_option_type',
            '_custom_option_title',
            '_custom_option_is_required',
            '_custom_option_price',
            '_custom_option_sku',
            '_custom_option_max_characters',
            '_custom_option_sort_order',
            '_custom_option_row_title',
            '_custom_option_row_price',
            '_custom_option_row_sku',
            '_custom_option_row_sort',
            '_associated_sku',
            '_associated_default_qty',
            '_associated_position',
            'bundle_configurations'
        ];

        parent::__construct(
            $data
        );
    }

    /**
     * Get special attributes
     *
     * @return string[]
     */
    public function getSpecialAttributes()
    {
        return $this->importProduct->getSpecialAttributes();
    }

    /**
     * Prepare row method
     *
     * @param  [] $rowData
     *
     * @return []
     */
    abstract public function prepareRow($rowData);

    abstract public function prepareColumns($rowData);

    abstract public function afterColumns($rowData, $maps);

    public function formatCustomOptions($rowData)
    {
        return $rowData;
    }

    public function deleteColumns($array)
    {
        return $this->unsetColumns($array, $this->unsetColumns);
    }

    protected function deleteEmpty($array)
    {
        if (isset($array['sku'])) {
            unset($array['sku']);
        }
        $newElement = [];
        foreach ($array as $key => $element) {
            if (strlen($element)) {
                $newElement[$key] = $element;
            }
        }

        return $newElement;
    }

    protected function mergeData($rowData, $prevData, $separator)
    {

        $data = $this->deleteEmpty($rowData);
        foreach ($data as $key => $value) {
            $prevData[$key] .= $separator . $value;
        }

        return $prevData;
    }

    /**
     * Check if platform is gateway
     *
     * @return bool
     */
    public function isGateway()
    {
        return false;
    }
}
