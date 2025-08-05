<?php
declare(strict_types=1);
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Import;

use function array_diff;
use function array_keys;
use function array_map;
use function array_merge;
use function array_values;
use Exception;
use function explode;
use Firebear\ImportExport\Api\UrlKeyManagerInterface;
use Firebear\ImportExport\Helper\Additional;
use Firebear\ImportExport\Helper\Data as FirebearImportExportData;
use Firebear\ImportExport\Model\Cache\Type\ImportProduct as ImportProductCache;
use Firebear\ImportExport\Model\Export\RowCustomizer\ProductVideo;
use Firebear\ImportExport\Model\Import;
use Firebear\ImportExport\Model\Import\Product\CategoryProcessor;
use Firebear\ImportExport\Model\Import\Product\Image as ImportImage;
use Firebear\ImportExport\Model\Import\Product\ImageProcessor as ImportImageProcessor;
use Firebear\ImportExport\Model\Import\Product\Integration\IntegrationInterface;
use Firebear\ImportExport\Model\Import\Product\Integration\MageArrayMarketplace;
use Firebear\ImportExport\Model\Import\Product\Integration\WebkulMarketplace;
use Firebear\ImportExport\Model\Import\Product\MediaVideoGallery;
use Firebear\ImportExport\Model\Import\Product\OptionFactory;
use Firebear\ImportExport\Model\Import\Product\Price\Rule\ConditionFactory as ConditionFactoryAlias;
use Firebear\ImportExport\Model\Import\Product\Type\Downloadable;
use Firebear\ImportExport\Model\Job;
use Firebear\ImportExport\Model\ResourceModel\Job\CollectionFactory;
use Firebear\ImportExport\Model\Source\Import\Config;
use Firebear\ImportExport\Model\Source\Type\AbstractType;
use Firebear\ImportExport\Model\Translation\Translator;
use Firebear\ImportExport\Traits\Import\Entity as ImportTrait;
use Firebear\ImportExport\Ui\Component\Listing\Column\Entity\Import\Attributes\SystemOptions;
use Firebear\ImportExport\Ui\Component\Listing\Column\Import\Source\Configurable\Type\Options as TypeOptions;
use function implode;
use function in_array;
use function interface_exists;
use InvalidArgumentException;
use function is_array;
use function is_int;
use Magento\Bundle\Model\Product\Price as BundlePrice;
use Magento\BundleImportExport\Model\Import\Product\Type\Bundle;
use Magento\Catalog\Api\Data\CategoryLinkInterface;
use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Helper\Data as CatalogHelperData;
use Magento\Catalog\Helper\Product as CatalogHelperProduct;
use Magento\Catalog\Model\CategoryLinkRepository;
use Magento\Catalog\Model\Config as CatalogConfig;
use Magento\Catalog\Model\Product\Action;
use Magento\Catalog\Model\Product\ActionFactory;
use Magento\Catalog\Model\Product\Attribute\Backend\Sku;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Catalog\Model\Product\Link as ProductLink;
use Magento\Catalog\Model\Product\Media\ConfigInterface;
use Magento\Catalog\Model\Product\Url;
use Magento\Catalog\Model\Product\Visibility;
use Magento\Catalog\Model\ResourceModel\Eav\AttributeFactory;
use Magento\Catalog\Model\ResourceModel\Product\Collection;
use Magento\Catalog\Model\ResourceModel\Product\CollectionFactory as ProductCollectionFactory;
use Magento\Catalog\Model\ResourceModel\Product\LinkFactory;
use Magento\CatalogImportExport\Model\Import\Product as MagentoProduct;
use Magento\CatalogImportExport\Model\Import\Product\ImageTypeProcessor;
use Magento\CatalogImportExport\Model\Import\Product\MediaGalleryProcessor;
use Magento\CatalogImportExport\Model\Import\Product\RowValidatorInterface as ValidatorInterface;
use Magento\CatalogImportExport\Model\Import\Product\SkuProcessor;
use Magento\CatalogImportExport\Model\Import\Product\StoreResolver;
use Magento\CatalogImportExport\Model\Import\Product\TaxClassProcessor;
use Magento\CatalogImportExport\Model\Import\Product\Type\Factory;
use Magento\CatalogImportExport\Model\Import\Product\Validator;
use Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModel;
use Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModelFactory;
use Magento\CatalogImportExport\Model\Import\Proxy\ProductFactory;
use Magento\CatalogImportExport\Model\StockItemImporterInterface;
use Magento\CatalogInventory\Api\StockConfigurationInterface;
use Magento\CatalogInventory\Api\StockRegistryInterface;
use Magento\CatalogInventory\Model\ResourceModel\Stock\Item;
use Magento\CatalogInventory\Model\ResourceModel\Stock\ItemFactory;
use Magento\CatalogInventory\Model\Spi\StockStateProviderInterface;
use Magento\Customer\Model\GroupFactory;
use Magento\Eav\Model\Entity\Attribute\AbstractAttribute;
use Magento\Eav\Model\EntityFactory;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\Group\CollectionFactory as AttributeGroupCollectionFactory;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\Set\CollectionFactory as AttributeSetCollectionFactory;
use Magento\Framework\App\CacheInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\App\ProductMetadataInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Adapter\ConnectionException;
use Magento\Framework\DB\Adapter\DeadlockException;
use Magento\Framework\DB\Adapter\LockWaitException;
use Magento\Framework\Event\ManagerInterface;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Exception\NoSuchEntityException;
use Magento\Framework\Filesystem;
use Magento\Framework\Indexer\IndexerRegistry;
use Magento\Framework\Intl\DateTimeFactory;
use Magento\Framework\MessageQueue\PublisherInterface;
use Magento\Framework\Model\ResourceModel\Db\ObjectRelationProcessor;
use Magento\Framework\Model\ResourceModel\Db\TransactionManagerInterface;
use Magento\Framework\Module\Manager;
use Magento\Framework\Stdlib\DateTime;
use Magento\Framework\Stdlib\DateTime\TimezoneInterface;
use Magento\ImportExport\Model\Import\Config as ImportConfig;
use Magento\ImportExport\Model\Import\Entity\AbstractEntity;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingError;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Magento\Store\Model\ScopeInterface;
use Magento\Store\Model\Store;
use Magento\Store\Model\StoreManagerInterface;
use Magento\Store\Model\WebsiteFactory;
use Magento\Swatches\Helper\Data;
use Magento\Swatches\Helper\Media;
use Magento\Swatches\Model\ResourceModel\Swatch\CollectionFactory as SwatchCollectionFactory;
use Magento\Swatches\Model\Swatch;
use Magento\Tax\Model\ClassModel;
use Magento\Tax\Model\ResourceModel\TaxClass\CollectionFactory as TaxClassCollectionFactory;
use function mb_strtolower;
use function strlen;
use function strtolower;
use function substr;
use Symfony\Component\Console\Output\ConsoleOutput;
use function version_compare;
use Zend\Serializer\Serializer;
use Zend_Db_Select;
use Zend_Validate_Exception;
use Zend_Validate_Regex;

/**
 * Import entity product model
 *
 * @api
 *
 * @SuppressWarnings(PHPMD.TooManyFields)
 * @SuppressWarnings(PHPMD.ExcessiveClassComplexity)
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 * @since 100.0.2
 */
class Product extends MagentoProduct
{
    use ImportTrait;

    /**
     * Default website id
     */
    const DEFAULT_WEBSITE_ID = 1;
    /**
     * Used when create new attributes in column name
     */
    const ATTRIBUTE_SET_GROUP = 'attribute_set_group';
    /**
     * Attribute sets column name
     */
    const ATTRIBUTE_SET_COLUMN = 'attribute_set';

    /**
     * Maximum number of database retries
     */
    const MAX_DB_RETRIES = 5;

    protected $mediaProcessor;

    protected $classTaxNames = [];

    /**
     * @var ProductMetadataInterface
     */
    public $productMetadata;
    public $onlyUpdate = 0;
    protected $onlyAdd = false;
    public static $addFields = [
        'manage_stock',
        'use_config_manage_stock',
        'qty',
        'out_of_stock_qty',
        'min_qty',
        'use_config_min_qty',
        'min_sale_qty',
        'use_config_min_sale_qty',
        'max_sale_qty',
        'use_config_max_sale_qty',
        'is_qty_decimal',
        'backorders',
        'use_config_backorders',
        'notify_stock_qty',
        'use_config_notify_stock_qty',
        'enable_qty_increments',
        'use_config_enable_qty_inc',
        'qty_increments',
        'use_config_qty_increments',
        'is_in_stock',
        'low_stock_date',
        'stock_status_changed_auto',
        'is_decimal_divided',
        'has_options',
        'tax_class_name',
        self::COL_STORE_VIEW_CODE,
        'attribute_set_code',
        'configurable_variations',
        'configurable_variation_labels',
        'associated_skus',
        'base_image_label',
        'additional_images',
        'additional_image_labels',
        'small_image_label',
        'thumbnail_image_label',
        'swatch_image',
        'swatch_image_label',
        'remove_images',
        ProductVideo::VIDEO_URL_COLUMN,
        WebkulMarketplace::VENDOR_ID,
        WebkulMarketplace::COL_UNASSIGN_SELLER,
        MageArrayMarketplace::VENDOR_ID,
        MageArrayMarketplace::MAGE_PRICE_COMPARE,
        'additional_attributes',
        'custom_options'
    ];

    public static $specialAttributes = [
        self::COL_STORE,
        self::COL_ATTR_SET,
        self::COL_TYPE,
        self::COL_CATEGORY,
        self::COL_CATEGORY . '_position',
        'product_websites',
        self::COL_PRODUCT_WEBSITES,
        '_tier_price_website',
        '_tier_price_customer_group',
        '_tier_price_qty',
        '_tier_price_price',
        '_related_sku',
        '_related_position',
        '_crosssell_sku',
        '_crosssell_position',
        '_upsell_sku',
        '_upsell_position',
        '_custom_option_store',
        '_custom_option_type',
        '_custom_option_title',
        '_custom_option_is_required',
        '_custom_option_price',
        '_custom_option_sku',
        '_custom_option_max_characters',
        '_custom_option_sort_order',
        '_custom_option_file_extension',
        '_custom_option_image_size_x',
        '_custom_option_image_size_y',
        '_custom_option_row_title',
        '_custom_option_row_price',
        '_custom_option_row_sku',
        '_custom_option_row_sort',
        '_media_attribute_id',
        self::COL_MEDIA_IMAGE,
        '_media_label',
        '_media_position',
        '_media_is_disabled',
        '_tier_price_value_type',
        'product_online',
        'update_attribute_set',
    ];

    /**
     * @var UploaderFactory
     */
    protected $_uploaderFactory;

    /**
     * @var Product\CategoryProcessor
     */
    protected $categoryProcessor;

    /**
     * @var FirebearImportExportData
     */
    protected $helper;
    /**
     * @var Additional
     */
    protected $additional;
    /**
     * @var AbstractType
     */
    protected $sourceType;
    /**
     * @var AttributeFactory
     */
    protected $attributeFactory;
    /**
     * @var EntityFactory
     */
    protected $eavEntityFactory;
    /**
     * @var AttributeGroupCollectionFactory
     */
    protected $groupCollectionFactory;
    /**
     * @var array
     */
    protected $_attributeSetGroupCache;
    /**
     * @var CatalogHelperProduct
     */
    protected $productHelper;

    protected $_debugMode;
    /**
     * @var Config
     */
    protected $fireImportConfig;
    /**
     * @var ProductRepositoryInterface
     */
    protected $productRepository;

    /**
     * @var ProductCollectionFactory
     */
    protected $collectionFactory;

    /**
     * @var Data
     */
    protected $swatchesHelperData;

    /**
     * Helper to move image from tmp to catalog
     *
     * @var Media
     */
    protected $swatchHelperMedia;

    /**
     * @var SwatchCollectionFactory
     */
    protected $swatchCollectionFactory;

    /**
     * @var ConfigInterface
     */
    protected $mediaConfig;
    /**
     * @var GroupFactory
     */
    protected $groupFactory;
    /**
     * @var WebsiteFactory
     */
    protected $websiteFactory;

    /**
     * @var TaxClassCollectionFactory
     */
    protected $collectionTaxFactory;
    /**
     * @var StoreManagerInterface
     */
    protected $storeManager;
    protected $notValidedSku = [];
    protected $productLinkData = [];
    protected $productLink;
    protected $priceRuleConditionFactory;
    protected $platform;
    /** @var Manager */
    protected $manager;
    /**
     * @var UrlKeyManagerInterface
     */
    protected $urlKeyManager;
    /**
     * @var ActionFactory
     */
    protected $productActionFactory;

    private $cachedSwatchOptions = [];
    private $importCollection;
    private $_isRowCategoryMapped;
    private $lastSku;

    /**
     * Product entity identifier field
     *
     * @var string
     */
    private $productEntityIdentifierField;

    /**
     * Product entity link field
     *
     * @var string
     */
    private $productEntityLinkField;

    /** @var Translator */
    protected $translator;

    /**
     *
     * @var CategoryLinkRepository
     */
    protected $categoryLinkRepository;

    /**
     * Stock Item Importer
     *
     * @var StockItemImporterInterface
     */
    private $stockItemImporter;

    protected $categoryProductPosition = [];

    /**
     * @var ProductCollectionFactory
     */
    private $productCollectionFactory;

    /** @var array */
    protected $integrations;

    /**
     * @var PublisherInterface
     */
    protected $publisher;

    /**
     * @var ImportImage
     */
    protected $importImage;

    /**
     * @var ImportImageProcessor
     */
    protected $importImageProcessor;

    /**
     * @var CacheInterface
     */
    protected $cache;

    /**
     * @var array
     */
    protected $originalImportRows = [];

    /**
     * @var array
     */
    protected $getProductIdByRowId = [];

    /**
     * @var array
     */
    protected $adminAttributeValue = [];

    /**
     * Product constructor.
     * @param Context $context
     * @param FirebearImportExportData $helper
     * @param Additional $additional
     * @param ManagerInterface $eventManager
     * @param StockRegistryInterface $stockRegistry
     * @param StockConfigurationInterface $stockConfiguration
     * @param StockStateProviderInterface $stockStateProvider
     * @param CatalogHelperData $catalogData
     * @param ImportConfig $importConfig
     * @param Config $fireImportConfig
     * @param ResourceModelFactory $resourceFactory
     * @param OptionFactory $optionFactory
     * @param AttributeSetCollectionFactory $setColFactory
     * @param Factory $productTypeFactory
     * @param LinkFactory $linkFactory
     * @param ProductFactory $proxyProdFactory
     * @param Filesystem $filesystem
     * @param ItemFactory $stockResItemFac
     * @param TimezoneInterface $localeDate
     * @param DateTime $dateTime
     * @param IndexerRegistry $indexerRegistry
     * @param StoreResolver $storeResolver
     * @param SkuProcessor $skuProcessor
     * @param Validator $validator
     * @param ObjectRelationProcessor $objectRelationProcessor
     * @param TransactionManagerInterface $transactionManager
     * @param TaxClassProcessor $taxClassProcessor
     * @param ScopeConfigInterface $scopeConfig
     * @param Url $productUrl
     * @param AttributeFactory $attributeFactory
     * @param EntityFactory $eavEntityFactory
     * @param AttributeGroupCollectionFactory $groupCollectionFactory
     * @param CatalogHelperProduct $productHelper
     * @param ProductMetadataInterface $productMetadata
     * @param ProductRepositoryInterface $productRepository
     * @param ProductCollectionFactory $collectionFactory
     * @param GroupFactory $groupFactory
     * @param WebsiteFactory $websiteFactory
     * @param CategoryProcessor $categoryProcessor
     * @param UploaderFactory $uploaderFactory
     * @param TaxClassCollectionFactory $collectionTaxFactory
     * @param StoreManagerInterface $storeManager
     * @param CollectionFactory $importCollectionFactory
     * @param ConditionFactoryAlias $priceRuleConditionFactory
     * @param Data $swatchesHelperData
     * @param Media $swatchHelperMedia
     * @param SwatchCollectionFactory $swatchCollectionFactory
     * @param ConfigInterface $mediaConfig
     * @param Manager $moduleManager
     * @param ProductLink $productLink
     * @param UrlKeyManagerInterface $urlKeyManager
     * @param CategoryLinkRepository $categoryLinkRepository
     * @param ActionFactory $productActionFactory
     * @param Translator $translator
     * @param array $data
     * @param array $integrations
     * @param array $dateAttrCodes
     * @param CatalogConfig|null $catalogConfig
     * @param DateTimeFactory|null $dateTimeFactory
     * @throws LocalizedException
     * @throws Exception
     */
    public function __construct(
        Context $context,
        ImportImage $importImage,
        ImportImageProcessor $importImageProcessor,
        FirebearImportExportData $helper,
        CacheInterface $cache,
        Additional $additional,
        ManagerInterface $eventManager,
        StockRegistryInterface $stockRegistry,
        StockConfigurationInterface $stockConfiguration,
        StockStateProviderInterface $stockStateProvider,
        CatalogHelperData $catalogData,
        ImportConfig $importConfig,
        Config $fireImportConfig,
        ResourceModelFactory $resourceFactory,
        Product\OptionFactory $optionFactory,
        AttributeSetCollectionFactory $setColFactory,
        Factory $productTypeFactory,
        LinkFactory $linkFactory,
        ProductFactory $proxyProdFactory,
        Filesystem $filesystem,
        ItemFactory $stockResItemFac,
        TimezoneInterface $localeDate,
        DateTime $dateTime,
        IndexerRegistry $indexerRegistry,
        StoreResolver $storeResolver,
        SkuProcessor $skuProcessor,
        Validator $validator,
        ObjectRelationProcessor $objectRelationProcessor,
        TransactionManagerInterface $transactionManager,
        TaxClassProcessor $taxClassProcessor,
        ScopeConfigInterface $scopeConfig,
        Url $productUrl,
        AttributeFactory $attributeFactory,
        EntityFactory $eavEntityFactory,
        AttributeGroupCollectionFactory $groupCollectionFactory,
        CatalogHelperProduct $productHelper,
        ProductMetadataInterface $productMetadata,
        ProductRepositoryInterface $productRepository,
        ProductCollectionFactory $collectionFactory,
        GroupFactory $groupFactory,
        WebsiteFactory $websiteFactory,
        Product\CategoryProcessor $categoryProcessor,
        UploaderFactory $uploaderFactory,
        TaxClassCollectionFactory $collectionTaxFactory,
        StoreManagerInterface $storeManager,
        CollectionFactory $importCollectionFactory,
        Product\Price\Rule\ConditionFactory $priceRuleConditionFactory,
        Data $swatchesHelperData,
        Media $swatchHelperMedia,
        SwatchCollectionFactory $swatchCollectionFactory,
        ConfigInterface $mediaConfig,
        Manager $moduleManager,
        ProductLink $productLink,
        UrlKeyManagerInterface $urlKeyManager,
        CategoryLinkRepository $categoryLinkRepository,
        ActionFactory $productActionFactory,
        Translator $translator,
        array $data = [],
        array $integrations = [],
        array $dateAttrCodes = [],
        CatalogConfig $catalogConfig = null,
        DateTimeFactory $dateTimeFactory = null
    ) {
        $this->output = $context->getOutput();
        $this->importImage = $importImage;
        $this->cache = $cache;
        $this->helper = $helper;
        $this->importImageProcessor = $importImageProcessor;
        $this->attributeFactory = $attributeFactory;
        $this->eavEntityFactory = $eavEntityFactory;
        $this->groupCollectionFactory = $groupCollectionFactory;
        $this->productHelper = $productHelper;
        $this->additional = $additional;
        $this->fireImportConfig = $fireImportConfig;
        $this->groupFactory = $groupFactory;
        $this->storeManager = $storeManager;
        $this->priceRuleConditionFactory = $priceRuleConditionFactory;
        $this->swatchesHelperData = $swatchesHelperData;
        $this->swatchHelperMedia = $swatchHelperMedia;
        $this->swatchCollectionFactory = $swatchCollectionFactory;
        $this->mediaConfig = $mediaConfig;
        $this->translator = $translator;
        $this->productLink = $productLink;

        if (class_exists(MediaGalleryProcessor::class)) {
            $this->mediaProcessor = ObjectManager::getInstance()
                ->get(MediaVideoGallery::class);
            $mediaProcessor = ObjectManager::getInstance()
                ->get(MediaGalleryProcessor::class);
        }

        if (interface_exists(StockItemImporterInterface::class)) {
            $this->stockItemImporter = ObjectManager::getInstance()
                ->get(StockItemImporterInterface::class);
        }
        if (class_exists(ImageTypeProcessor::class)) {
            $imageTypeProcessor = ObjectManager::getInstance()
                ->get(ImageTypeProcessor::class);
        }
        if (interface_exists(PublisherInterface::class)) {
            $this->publisher = ObjectManager::getInstance()
                ->get(PublisherInterface::class);
        }
        $this->setSerializer($context->getSerializer());
        $this->setLogger($context->getLogger());
        if (version_compare($productMetadata->getVersion(), '2.3.0', '>=')) {
            parent::__construct(
                $context->getJsonHelper(),
                $context->getImportExportData(),
                $context->getDataSourceModel(),
                $context->getConfig(),
                $context->getResource(),
                $context->getResourceHelper(),
                $context->getStringUtils(),
                $context->getErrorAggregator(),
                $eventManager,
                $stockRegistry,
                $stockConfiguration,
                $stockStateProvider,
                $catalogData,
                $importConfig,
                $resourceFactory,
                $optionFactory,
                $setColFactory,
                $productTypeFactory,
                $linkFactory,
                $proxyProdFactory,
                $uploaderFactory,
                $filesystem,
                $stockResItemFac,
                $localeDate,
                $dateTime,
                $context->getLogger(),
                $indexerRegistry,
                $storeResolver,
                $skuProcessor,
                $categoryProcessor,
                $validator,
                $objectRelationProcessor,
                $transactionManager,
                $taxClassProcessor,
                $scopeConfig,
                $productUrl,
                $data,
                $dateAttrCodes,
                $catalogConfig,
                $imageTypeProcessor,
                $mediaProcessor,
                $this->stockItemImporter,
                $dateTimeFactory,
                $productRepository
            );
        } elseif (version_compare($productMetadata->getVersion(), '2.2.3', '<=')) {
            parent::__construct(
                $context->getJsonHelper(),
                $context->getImportExportData(),
                $context->getDataSourceModel(),
                $context->getConfig(),
                $context->getResource(),
                $context->getResourceHelper(),
                $context->getStringUtils(),
                $context->getErrorAggregator(),
                $eventManager,
                $stockRegistry,
                $stockConfiguration,
                $stockStateProvider,
                $catalogData,
                $importConfig,
                $resourceFactory,
                $optionFactory,
                $setColFactory,
                $productTypeFactory,
                $linkFactory,
                $proxyProdFactory,
                $uploaderFactory,
                $filesystem,
                $stockResItemFac,
                $localeDate,
                $dateTime,
                $context->getLogger(),
                $indexerRegistry,
                $storeResolver,
                $skuProcessor,
                $categoryProcessor,
                $validator,
                $objectRelationProcessor,
                $transactionManager,
                $taxClassProcessor,
                $scopeConfig,
                $productUrl,
                $data,
                $dateAttrCodes,
                $catalogConfig
            );
        } else {
            parent::__construct(
                $context->getJsonHelper(),
                $context->getImportExportData(),
                $context->getDataSourceModel(),
                $context->getConfig(),
                $context->getResource(),
                $context->getResourceHelper(),
                $context->getStringUtils(),
                $context->getErrorAggregator(),
                $eventManager,
                $stockRegistry,
                $stockConfiguration,
                $stockStateProvider,
                $catalogData,
                $importConfig,
                $resourceFactory,
                $optionFactory,
                $setColFactory,
                $productTypeFactory,
                $linkFactory,
                $proxyProdFactory,
                $uploaderFactory,
                $filesystem,
                $stockResItemFac,
                $localeDate,
                $dateTime,
                $context->getLogger(),
                $indexerRegistry,
                $storeResolver,
                $skuProcessor,
                $categoryProcessor,
                $validator,
                $objectRelationProcessor,
                $transactionManager,
                $taxClassProcessor,
                $scopeConfig,
                $productUrl,
                $data,
                $dateAttrCodes,
                $catalogConfig,
                $mediaProcessor
            );
        }
        $this->_debugMode = $helper->getDebugMode();
        $this->productMetadata = $productMetadata;
        $this->productRepository = $productRepository;
        $this->collectionFactory = $collectionFactory;
        $this->websiteFactory = $websiteFactory;
        $this->collectionTaxFactory = $collectionTaxFactory;
        $this->importCollection = $importCollectionFactory;
        $this->_specialAttributes[] = '_tier_price_value_type';
        $this->_fieldsMap += [
            '_tier_price_website' => 'tier_price_website',
            '_tier_price_customer_group' => 'tier_price_customer_group',
            '_tier_price_qty' => 'tier_price_qty',
            '_tier_price_price' => 'tier_price_price',
            '_tier_price_value_type' => 'tier_price_value_type',
        ];
        $this->_isRowCategoryMapped = false;
        $this->manager = $moduleManager;
        $this->urlKeyManager = $urlKeyManager;
        $this->productActionFactory = $productActionFactory;
        $this->categoryLinkRepository = $categoryLinkRepository;
        $this->productCollectionFactory = $collectionFactory;
        foreach ($integrations as $integration) {
            if (!($integration instanceof IntegrationInterface)) {
                throw new LocalizedException(__(
                    'Integration class must be instance of "%interface"',
                    ['interface' => IntegrationInterface::class]
                ));
            }
        }
        $this->integrations = $integrations;
        $this->_fireConstruct();
    }

    /**
     * @return string
     * @throws Exception
     */
    public function _getProductEntityLinkField()
    {
        return $this->getProductEntityLinkField();
    }

    /**
     * @return $this
     * @throws Exception
     */
    public function _fireConstruct()
    {
        $this->urlKeyManager
            ->setEntity($this)
            ->initUrlKeys();
        return $this;
    }

    /**
     * Remove large objects
     */
    public function __destruct()
    {
        unset($this->_optionEntity);
    }

    /**
     * import product data
     *
     * @return bool
     * @throws Exception
     * @throws LocalizedException
     */
    public function importData()
    {
        $this->notValidedSku = [];
        if ($this->_parameters['behavior'] == Import::FIREBEAR_ONLY_UPDATE) {
            $this->onlyUpdate = 1;
            $this->_parameters['behavior'] = Import::BEHAVIOR_APPEND;
        } elseif ($this->_parameters['behavior'] == Import::FIREBEAR_ONLY_ADD) {
            $this->onlyAdd = true;
            $this->_parameters['behavior'] = Import::BEHAVIOR_APPEND;
        }
        $this->_validatedRows = null;

        if (Import::BEHAVIOR_REPLACE == $this->getBehavior()) {
            $this->_replaceFlag = true;
            $this->replaceProducts();
        } elseif (Import::BEHAVIOR_DELETE == $this->getBehavior()) {
            $this->_deleteProducts();
        } else {
            $this->saveProductsData();
        }
        $this->_eventManager->dispatch('catalog_product_import_finish_before', ['adapter' => $this]);

        return true;
    }

    /**
     * Delete products.
     *
     * @return $this
     * @throws Exception
     */
    protected function _deleteProducts()
    {
        $productEntityTable = $this->_resourceFactory->create()->getEntityTable();

        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $idsToDelete = [];
            foreach ($bunch as $rowNum => $rowData) {
                $validate = $this->validateRow($rowData, $rowNum);
                $scopeStore = self::SCOPE_STORE == $this->getRowScope($rowData);
                $scopeDefault = self::SCOPE_DEFAULT == $this->getRowScope($rowData);
                if ($validate && ($scopeStore || $scopeDefault)) {
                    $oldSku = $this->getExistingSku($rowData[self::COL_SKU])['entity_id'];
                    if (!empty($oldSku)) {
                        $idsToDelete[] = $oldSku;
                    }
                }
            }
            if ($idsToDelete) {
                $this->countItemsDeleted += count($idsToDelete);
                $this->_processedEntitiesCount += count($idsToDelete);
                $this->transactionManager->start($this->_connection);
                try {
                    $this->objectRelationProcessor->delete(
                        $this->transactionManager,
                        $this->_connection,
                        $productEntityTable,
                        $this->_connection->quoteInto('entity_id IN (?)', $idsToDelete),
                        ['entity_id' => $idsToDelete]
                    );
                    $this->_eventManager->dispatch(
                        'catalog_product_import_bunch_delete_commit_before',
                        [
                            'adapter' => $this,
                            'bunch' => $bunch,
                            'ids_to_delete' => $idsToDelete,
                        ]
                    );
                    $this->transactionManager->commit();
                } catch (Exception $e) {
                    $this->transactionManager->rollBack();
                    throw $e;
                }
                $this->_eventManager->dispatch(
                    'catalog_product_import_bunch_delete_after',
                    ['adapter' => $this, 'bunch' => $bunch]
                );
            }
        }
        return $this;
    }

    /**
     * Replace imported products.
     *
     * @return $this
     * @throws LocalizedException
     * @throws Zend_Validate_Exception
     */
    protected function replaceProducts()
    {
        $this->deleteProductsForReplacement();
        $this->_oldSku = $this->skuProcessor->reloadOldSkus()->getOldSkus();
        $this->_validatedRows = null;
        $this->setParameters(
            array_merge(
                $this->getParameters(),
                ['behavior' => Import::BEHAVIOR_APPEND]
            )
        );
        $this->saveProductsData();

        return $this;
    }

    /**
     * find url_key duplicates
     *
     * @param array $bunchRows
     * @return array $bunchRows
     */
    protected function findUrlKeyDuplicates($bunchRows)
    {
        $urlKeys = [];
        foreach ($bunchRows as $rowNum => $rowData) {
            if (array_key_exists(self::COL_STORE_VIEW_CODE, $rowData)
                && $rowData[self::COL_STORE_VIEW_CODE] === null
            ) {
                $storeCode = 'admin';
            } else {
                $storeCode = $rowData[self::COL_STORE_VIEW_CODE] ?? 'default';
            }

            if (!isset($urlKeys[$storeCode])) {
                $urlKeys[$storeCode] = [];
            }

            if (isset($rowData[self::URL_KEY]) && in_array($rowData[self::URL_KEY], $urlKeys[$storeCode])) {
                unset($bunchRows[$rowNum]);
                $message = 'product with sku: %1 not imported because its url is not unique.';
                $this->addLogWriteln(__($message, $this->getCorrectSkuAsPerLength($rowData)), $this->output, 'info');
            } else {
                $urlKeys[$storeCode][] = $rowData[self::URL_KEY] ?? '';
            }
        }

        return $bunchRows;
    }

    /**
     * Save products data.
     *
     * @return $this
     * @throws LocalizedException
     * @throws Zend_Validate_Exception
     */
    protected function saveProductsData()
    {
        foreach ($this->_productTypeModels as $productTypeModel) {
            if ($productTypeModel instanceof Downloadable) {
                $productTypeModel->clearObject();
            }
        }
        $this->saveProducts();
        foreach ($this->_productTypeModels as $productTypeModel) {
            $productTypeModel->saveData();
        }

        $this->_saveLinks();
        $this->_saveStockItem();

        if ($this->_replaceFlag) {
            $this->getOptionEntity()->clearProductsSkuToId();
        }
        $this->getOptionEntity()->importData();
        $verbosity = false;
        if (!$this->helper->getProcessor()->inConsole) {
            $verbosity = ConsoleOutput::VERBOSITY_VERBOSE;
        }
        if (is_array($this->integrations)) {
            /**
             * @var $moduleKey
             * @var Import\Product\Integration\AbstractIntegration $integration
             */
            foreach ($this->integrations as $moduleKey => $integration) {
                if ($this->manager->isEnabled($moduleKey)) {
                    $integration->setAdapter($this);
                    $integration->setDataSourceModel($this->_dataSourceModel);
                    $integration->importData($verbosity);
                }
            }
        }
        foreach ($this->productLinkData as $idConfigProduct => $typesLink) {
            $this->addProductLinks($idConfigProduct, $typesLink);
        }

        return $this;
    }

    /**
     * Save Stock Item.
     *
     * @return $this
     * @throws LocalizedException
     */
    protected function _saveStockItem()
    {
        /** @var $stockResource Item */
        $stockResource = $this->_stockResItemFac->create();
        $entityTable = $stockResource->getMainTable();
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $stockData = [];
            // Format bunch to stock data rows
            foreach ($bunch as $rowNum => $rowData) {
                if (!$this->isRowAllowedToImport($rowData, $rowNum)) {
                    continue;
                }

                if (isset($this->_parameters['increase_product_stock_by_qty'])
                    && $this->_parameters['increase_product_stock_by_qty'] == 1
                ) {
                    $rowData['qty'] = (int)($rowData['qty'] + $this->getProductStockQty($rowData[self::COL_SKU]));
                }

                $sku = $rowData[self::COL_SKU];
                $row = [];
                if ($this->skuProcessor->getNewSku($sku) !== null) {
                    $row = $this->formatStockDataForRow($rowData);
                }

                if (!isset($stockData[$sku])) {
                    $stockData[$sku] = $row;
                }
            }

            // Insert rows
            if (!empty($stockData)) {
                if ($this->stockItemImporter instanceof StockItemImporterInterface
                    && version_compare($this->productMetadata->getVersion(), '2.3.0', '>=')
                ) {
                    try {
                        $this->stockItemImporter->import($stockData);
                    } catch (Exception $exception) {
                        $this->addLogWriteln($exception->getMessage(), $this->getOutput(), 'info');
                        $this->getLogger()->debug($exception);
                    }
                } else {
                    $this->_connection->insertOnDuplicate($entityTable, array_values($stockData));
                }
            }
        }
        return $this;
    }

    /**
     * Format row data to DB compatible values
     *
     * @param array $rowData
     * @return array
     * @throws Exception
     */
    private function formatStockDataForRow(array $rowData)
    {
        $sku = $rowData[self::COL_SKU];
        $row['product_id'] = $this->skuProcessor->getNewSku($sku)['entity_id'];
        $row['website_id'] = $this->stockConfiguration->getDefaultScopeId();
        $row['stock_id'] = $this->stockRegistry->getStock($row['website_id'])->getStockId();

        /** @var \Magento\CatalogInventory\Model\Stock\Item $stockItemDo */
        $stockItemDo = $this->stockRegistry->getStockItem($row['product_id'], $row['website_id']);
        $existStockData = $stockItemDo->getData();

        $row = array_merge(
            $this->defaultStockData,
            array_intersect_key($existStockData, $this->defaultStockData),
            array_intersect_key($rowData, $this->defaultStockData),
            $row
        );

        $typeId = $this->skuProcessor->getNewSku($sku)['type_id'];
        if ($this->stockConfiguration->isQty($typeId)) {
            if ($this->manager->isEnabled('Magento_CatalogMessageBus')) {
                if (isset($existStockData['qty'])) {
                    $row['qty'] = $existStockData['qty'];
                }
            }
            $stockItemDo->setData($row);
            $row['is_in_stock'] = $stockItemDo->getBackorders() && isset($row['is_in_stock'])
                ? $row['is_in_stock']
                : $this->stockStateProvider->verifyStock($stockItemDo);
            if ($this->stockStateProvider->verifyNotification($stockItemDo)) {
                $row['low_stock_date'] = $this->dateTime->gmDate(
                    'Y-m-d H:i:s',
                    (new \DateTime())->getTimestamp()
                );
            }
            $row['stock_status_changed_auto'] =
                (int)!$this->stockStateProvider->verifyStock($stockItemDo);
        } else {
            $row['qty'] = 0;
        }

        return $row;
    }

    /**
     * get product qty
     *
     * @param string $sku
     *
     * @return float
     */
    protected function getProductStockQty($sku)
    {
        try {
            $stockItem = $this->stockRegistry->getStockItemBySku($sku);
            $qty = $stockItem->getData('qty');
        } catch (NoSuchEntityException $e) {
            $qty = 0;
        }

        return $qty;
    }

    /**
     * Set valid attribute set and product type to rows with all scopes
     * to ensure that existing products doesn't changed.
     *
     * @param array $rowData
     *
     * @return array
     */
    protected function _prepareRowForDb(array $rowData)
    {
        $productType = isset($rowData[self::COL_TYPE]) ? $rowData[self::COL_TYPE] : '';
        $rowData = parent::_prepareRowForDb($rowData);
        if ($productType) {
            $rowData[self::COL_TYPE] = $productType;
        }
        if (!$this->onlyUpdate) {
            foreach ($this->defaultStockData as $key => $value) {
                if (isset($rowData[$key])) {
                    if ($rowData[$key] === true) {
                        $rowData[$key] = 1;
                    } elseif ($rowData[$key] === false) {
                        $rowData[$key] = 0;
                    } elseif ($rowData[$key] === '') {
                        $rowData[$key] = 0;
                    } elseif ($rowData[$key] === null && $key != 'low_stock_date') {
                        $rowData[$key] = 0;
                    }
                } else {
                    if ($key == 'qty') {
                        $rowData[$key] = $this->getProductStockQty($rowData[self::COL_SKU]);
                    } else {
                        $rowData[$key] = $value;
                    }
                }
            }
        }
        $rowData = $this->adjustBundleTypeAttributes($rowData);
        return $rowData;
    }

    /**
     * @param array $rowData
     * @param array $configurableData
     */
    protected function prepareConfigurableVariation(array $rowData, array &$configurableData = [])
    {
        $confSwitch = $this->_parameters['configurable_switch'];

        if ($confSwitch && isset($rowData['product_type'])
            && ($rowData['product_type'] == 'simple' || $rowData['product_type'] == 'virtual')) {
            $field = $this->_parameters['configurable_field'];
            $skuConf = null;
            if (isset($rowData[$field])) {
                switch ($this->_parameters['configurable_type']) {
                    case TypeOptions::FIELD:
                        if ($rowData[$field] && $this->getCorrectSkuAsPerLength($rowData) != $rowData[$field]) {
                            $skuConf = $rowData[$field];
                        }
                        break;
                    case TypeOptions::PART_UP:
                        $array = explode($this->_parameters['configurable_part'], $rowData[$field]);
                        if (count($array) > 1) {
                            $skuConf = $array[0];
                        }
                        break;
                    case TypeOptions::PART_DOWN:
                        $array = explode($this->_parameters['configurable_part'], $rowData[$field]);
                        if (count($array) > 1) {
                            $skuConf = $array[count($array) - 1];
                        }
                        break;
                    case TypeOptions::SUB_UP:
                        $skuConf = substr($rowData[$field], 0, $this->_parameters['configurable_symbols']);
                        break;
                    case TypeOptions::SUB_DOWN:
                        $skuConf = substr($rowData[$field], -$this->_parameters['configurable_symbols']);
                        break;
                }
            }
            if ($this->_replaceFlag && !isset($this->_oldSku[mb_strtolower($skuConf)])) {
                $skuConf = null;
            }
            if ($skuConf) {
                $newData = $rowData;
                $arrayConf = [];
                if (!empty($this->_parameters['configurable_variations'])) {
                    foreach ($this->_parameters['configurable_variations'] as $attrField) {
                        if (isset($newData[$attrField]) && trim($newData[$attrField]) !== '') {
                            $arrayConf[$attrField] = $newData[$attrField];
                        }
                    }
                }
                if (!empty($arrayConf)) {
                    $arrayConf['sku'] = (string) $newData['sku'];
                    $configurableData[(string) $skuConf][] = $arrayConf;
                }
            }
        }
    }

    /**
     * Gather and save information about product entities.
     *
     * @return $this
     * @throws LocalizedException
     * @throws Zend_Validate_Exception
     * @throws Exception
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     * @SuppressWarnings(PHPMD.NPathComplexity)
     * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
     */
    protected function saveProducts()
    {
        $this->translator = $this->translator->init($this->_parameters);
        $existingImages = [];
        $existingUpload = [];
        $entityLinkField = $this->getProductEntityLinkField();
        if (!empty($this->_parameters['import_source']) && $this->_parameters['import_source'] != 'file') {
            $this->_initSourceType($this->_parameters['import_source']);
        }
        $configurableData = [];

        $isPriceGlobal = $this->_catalogData->isPriceGlobal();
        $productLimit = null;
        $productsQty = null;
        $this->importImage->setConfig($this->_parameters);

        while ($nextBunch = $this->_dataSourceModel->getNextBunch()) {
            $entityRowsIn = $entityRowsUp = [];
            $attributes = [];
            $this->websitesCache = $this->categoriesCache = $this->categoryProductPosition = [];
            $this->categoryProcessor->setRowCategoryPosition($this->categoryProductPosition);
            $mediaGallery = $uploadedImages = [];
            $tierPrices = [];
            $previousType = $prevAttributeSet = null;
            $existingImages = $this->getExistingImages($nextBunch);
            if ($this->sourceType && $this->_parameters['image_import_source']) {
                $nextBunch = $this->prepareImagesFromSource($nextBunch);
            }

            $prevData = [];
            $createValuesAllowed = (bool)$this->scopeConfig->getValue(
                Import::CREATE_ATTRIBUTES_CONF_PATH,
                ScopeInterface::SCOPE_STORE
            );
            $storeIds = $this->getStoreIds();
            foreach ($nextBunch as $rowNum => $rowData) {
                $time = explode(" ", microtime());
                $startTime = $time[0] + $time[1];
                if (isset($rowData[self::COL_CATEGORY])) {
                    $categoriesMapping = $this->categoriesMapping($rowData);
                    $rowData[self::COL_CATEGORY] = $categoriesMapping[self::COL_CATEGORY];

                    if (!empty($categoriesMapping[self::COL_CATEGORY . '_position'])) {
                        $rowData[self::COL_CATEGORY . '_position'] =
                            $categoriesMapping[self::COL_CATEGORY . '_position'];
                    }
                }

                $rowData = $this->joinIdenticalyData($rowData);
                if (isset($rowData['_attribute_set']) && isset($rowData['attribute_set_code'])) {
                    if (isset($rowData['update_attribute_set']) && ((int)($rowData['update_attribute_set']) > 0)) {
                        $rowData['_attribute_set'] = $rowData['attribute_set_code'];
                    } else {
                        unset($rowData['attribute_set_code']);
                    }
                }
                $oldSkus = $this->skuProcessor->getOldSkus();
                $sku = strtolower($this->getCorrectSkuAsPerLength($rowData));

                if (!isset($oldSkus[$sku])) {
                    if (!isset($rowData['_attribute_set'])
                        || (isset($rowData['_attribute_set']) && empty($rowData['_attribute_set']))
                    ) {
                        $collectSets = $this->_attrSetIdToName;
                        reset($collectSets);
                        $rowData['_attribute_set'] = current($collectSets);
                    }
                }

                if (isset($this->_parameters['remove_related_product'])
                    && $this->_parameters['remove_related_product'] == 1
                ) {
                    $this->removeRelatedProducts($this->getCorrectSkuAsPerLength($rowData));
                }

                if (isset($this->_parameters['remove_crosssell_product'])
                    && $this->_parameters['remove_crosssell_product'] == 1
                ) {
                    $this->removeCrosssellProducts($this->getCorrectSkuAsPerLength($rowData));
                }

                if (isset($this->_parameters['remove_upsell_product'])
                    && $this->_parameters['remove_upsell_product'] == 1
                ) {
                    $this->removeUpsellProducts($this->getCorrectSkuAsPerLength($rowData));
                }

                $rowData = $this->checkAdditionalImages($rowData);
                $rowData = $this->customChangeData($rowData);

                if (!$this->validateRow($rowData, $rowNum) || !$this->validateRowByProductType($rowData, $rowNum)) {
                    $this->addLogWriteln(
                        __('product with sku: %1 is not valided', $this->getCorrectSkuAsPerLength($rowData)),
                        $this->output,
                        'info'
                    );
                    $this->notValidedSku[] = strtolower($this->getCorrectSkuAsPerLength($rowData));
                    unset($nextBunch[$rowNum]);
                    continue;
                } else {
                    $rowData = $this->stripSlashes($rowData);
                }

                $productType = isset($rowData[self::COL_TYPE]) ?
                    strtolower($rowData[self::COL_TYPE]) :
                    $this->skuProcessor->getNewSku($this->getCorrectSkuAsPerLength($rowData))['type_id'];
                // custom
                if ($productType) {
                    $productTypeModel = $this->_productTypeModels[$productType];
                    if ($createValuesAllowed) {
                        $rowData = $this->createAttributeValues(
                            $productTypeModel,
                            $rowData
                        );
                    }
                }

                if (!isset($rowData[self::COL_ATTR_SET]) ||
                    !isset($this->_attrSetNameToId[$rowData[self::COL_ATTR_SET]])) {
                    $this->addRowError(ValidatorInterface::ERROR_INVALID_ATTR_SET, $rowNum);
                    $this->addLogWriteln(
                        __(
                            'product with sku: %1 is not valided. ' .
                            'Invalid value for Attribute Set column (set doesn\'t exist?)',
                            $this->getCorrectSkuAsPerLength($rowData)
                        ),
                        $this->output,
                        'info'
                    );
                    $this->notValidedSku[] = strtolower($this->getCorrectSkuAsPerLength($rowData));
                    unset($nextBunch[$rowNum]);
                    continue;
                }
                $urlKey = null;
                if (!($this->onlyUpdate && empty($rowData[self::URL_KEY]))) {
                    $urlKey = $this->getProductUrlKey($rowData);
                }

                if ($urlKey) {
                    if (!empty($rowData[self::URL_KEY])) {
                        // If url_key column and its value were in the CSV file
                        $rowData[self::URL_KEY] = $urlKey;
                    } elseif ($this->isNeedToChangeUrlKey($rowData)) {
                        // If url_key column was empty or even not declared in the CSV file but by the rules
                        // it is need to be setteed. In case when url_key is generating from name column we
                        // have to ensure that the bunch of products will pass for the event with url_key column.
                        $nextBunch[$rowNum][self::URL_KEY] = $rowData[self::URL_KEY] = $urlKey;
                    } elseif (isset($rowData[self::URL_KEY]) || isset($rowData[self::COL_NAME])) {
                        $rowData[self::URL_KEY] = $urlKey;
                    }
                }

                $this->urlKeys = [];
                $rowData = $this->applyCategoryLevelSeparator($rowData);
                $rowData = $this->adjustBundleTypeAttributes($rowData);

                if (empty($this->getCorrectSkuAsPerLength($rowData))) {
                    $rowData = array_merge($prevData, $this->deleteEmpty($rowData));
                } else {
                    $prevData = $rowData;
                }
                $sku = $this->getCorrectSkuAsPerLength($rowData);
                if ($this->onlyUpdate) {
                    $collectionUpdate = $this->collectionFactory->create()->addFieldToFilter(
                        self::COL_SKU,
                        $this->getCorrectSkuAsPerLength($rowData)
                    );
                    if (!$collectionUpdate->getSize()) {
                        $this->addLogWriteln(__('product with sku: %1 does not exist', $sku), $this->output, 'info');
                        unset($nextBunch[$rowNum]);
                        continue;
                    }
                }
                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                    unset($nextBunch[$rowNum]);
                    $this->notValidedSku[] = strtolower($this->getCorrectSkuAsPerLength($rowData));

                    continue;
                }

                if (isset($rowData['_attribute_set']) && isset($this->_attrSetNameToId[$rowData['_attribute_set']])) {
                    $this->skuProcessor->setNewSkuData(
                        $this->getCorrectSkuAsPerLength($rowData),
                        'attr_set_id',
                        $this->_attrSetNameToId[$rowData['_attribute_set']]
                    );
                }

                $this->prepareConfigurableVariation($rowData, $configurableData);
                $rowScope = $this->getRowScope($rowData);
                $rowSku = $this->getCorrectSkuAsPerLength($rowData);
                $checkSku = $rowSku;

                if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
                    $checkSku = strtolower($rowSku);
                }
                if (!$rowSku) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                    continue;
                } elseif (self::SCOPE_STORE == $rowScope) {
                    // set necessary data from SCOPE_DEFAULT row
                    $rowData[self::COL_TYPE] = $this->skuProcessor->getNewSku($checkSku)['type_id'];
                    $rowData['attribute_set_id'] = $this->skuProcessor->getNewSku($checkSku)['attr_set_id'];
                    $rowData[self::COL_ATTR_SET] = $this->skuProcessor->getNewSku($checkSku)['attr_set_code'];
                }

                // Entity phase
                if (!isset($this->_oldSku[$checkSku])) {
                    // new row
                    if (!$productLimit || $productsQty < $productLimit) {
                        if (isset($rowData['has_options'])) {
                            $hasOptions = $rowData['has_options'];
                        } else {
                            $hasOptions = 0;
                        }
                        $entityRowsIn[$rowSku] = [
                            'attribute_set_id' => $this->skuProcessor->getNewSku($checkSku)['attr_set_id'],
                            'type_id' => $this->skuProcessor->getNewSku($checkSku)['type_id'],
                            'sku' => $rowSku,
                            'has_options' => $hasOptions,
                            'created_at' => $this->_localeDate->date()->format(DateTime::DATETIME_PHP_FORMAT),
                            'updated_at' => $this->_localeDate->date()->format(DateTime::DATETIME_PHP_FORMAT),
                        ];
                        $productsQty++;
                    } else {
                        $rowSku = null;
                        // sign for child rows to be skipped
                        $this->getErrorAggregator()->addRowToSkip($rowNum);
                        continue;
                    }
                } else {
                    $array = [
                        'updated_at' => $this->_localeDate->date()->format(DateTime::DATETIME_PHP_FORMAT),
                        $entityLinkField => $this->_oldSku[$checkSku][$entityLinkField],
                    ];
                    $array['attribute_set_id'] = $this->skuProcessor->getNewSku($checkSku)['attr_set_id'];
                    $array['type_id'] = $productType;
                    // existing row
                    $entityRowsUp[] = $array;
                }

                // Categories phase
                if (!array_key_exists($rowSku, $this->categoriesCache)) {
                    $this->categoriesCache[$rowSku] = [];
                }

                $rowData['rowNum'] = $rowNum;
                $categoryIds = $this->getCategories($rowData);
                if (isset($rowData['category_ids'])) {
                    $catIds = explode($this->getMultipleValueSeparator(), $rowData['category_ids']);
                    $finalCatId = [];
                    foreach ($catIds as $catId) {
                        $catId = (int)$catId;
                        $existingCat = $this->categoryProcessor->getCategoryById($catId);
                        if (is_int($catId) && $catId > 0 && $existingCat && $existingCat->getId()) {
                            $finalCatId[] = $catId;
                        }
                    }
                    $categoryIds = array_merge($categoryIds, $finalCatId);
                }
                if (!empty($this->getCategoryProcessor()->getFailedCategories())) {
                    foreach ($this->getCategoryProcessor()->getFailedCategories() as $field) {
                        $this->addRowError('Category: ' . __($field['category']->getName() .
                                ' Url: ' . $field['category']->getUrlKey() .
                                ' ' . $field['exception']->getMessage()), $rowNum);
                    }
                    $this->addLogWriteln(
                        __('product with sku: %1 is not valided', $this->getCorrectSkuAsPerLength($rowData)),
                        $this->output,
                        'info'
                    );
                    $this->notValidedSku[] = strtolower($this->getCorrectSkuAsPerLength($rowData));
                    unset($nextBunch[$rowNum]);
                    continue;
                }
                foreach ($categoryIds as $id) {
                    $this->categoriesCache[$rowSku][$id] = true;
                }

                $catIds = [];
                if ($this->isSkuExist($rowSku)) {
                    if (!isset($this->_oldSku[strtolower($rowData[self::COL_SKU])]['entity_id'])) {
                        $entityId = $this->_oldSku[strtolower($rowData[self::COL_SKU])]['row_id'];
                        $this->skuProcessor->setNewSkuData($rowData[self::COL_SKU], 'entity_id', $entityId);
                    } else {
                        $entityId = $this->_oldSku[strtolower($rowData[self::COL_SKU])]['entity_id'];
                    }
                    $oldCategoryIds = $this->getCategoryLinks($entityId);

                    if (!empty($categoryIds)
                        && isset(
                            $this->_parameters['remove_product_categories'],
                            $this->_oldSku[strtolower($rowData[self::COL_SKU])]
                        )
                        && $this->_parameters['remove_product_categories'] > 0
                    ) {
                        foreach ($oldCategoryIds as $oldCategoryId) {
                            if (!in_array($oldCategoryId['category_id'], $categoryIds, false)) {
                                $this->categoriesCache[$rowSku][$oldCategoryId['category_id']] = false;
                            }
                        }
                    }
                }

                if (!isset($this->categoryProductPosition[$rowSku])) {
                    $this->categoryProductPosition[$rowSku] = [];
                }
                $this->categoryProductPosition[$rowSku] += $this->categoryProcessor->getRowCategoryPosition();

                if (isset($rowData[self::COL_CATEGORY]) && empty($rowData[self::COL_CATEGORY])) {
                    foreach ($catIds as $categoryId) {
                        $this->categoryLinkRepository->deleteByIds($categoryId, $rowData[self::COL_SKU]);
                        $this->categoriesCache[$rowSku] = [];
                        $this->categoryProductPosition[$rowSku] = [];
                    }
                }

                unset($rowData['rowNum']);
                if (!array_key_exists($rowSku, $this->websitesCache)) {
                    $this->websitesCache[$rowSku] = [];
                }
                // Product-to-Website phase
                if (!empty($rowData[self::COL_PRODUCT_WEBSITES])) {
                    $websiteCodes = explode($this->getMultipleValueSeparator(), $rowData[self::COL_PRODUCT_WEBSITES]);
                    foreach ($websiteCodes as $websiteCode) {
                        $websiteId = $this->storeResolver->getWebsiteCodeToId($websiteCode);
                        $this->websitesCache[$rowSku][$websiteId] = true;
                    }
                }
                // Price rules
                $rowData = $this->applyPriceRules($rowData);
                $fixedName = __("Fixed");
                $fixed = $fixedName;
                if (isset($rowData['_tier_price_value_type'])) {
                    $fixed = $rowData['_tier_price_value_type'] == $fixedName;
                }
                // Tier prices phase
                if (!empty($rowData['_tier_price_website'])) {
                    $tierPrices[$rowSku][] = [
                        'all_groups' => $rowData['_tier_price_customer_group'] == self::VALUE_ALL,
                        'customer_group_id' => $rowData['_tier_price_customer_group'] ==
                        self::VALUE_ALL ? 0 : $rowData['_tier_price_customer_group'],
                        'qty' => $rowData['_tier_price_qty'],
                        'value' => ($fixed) ? $rowData['_tier_price_price'] : 0,
                        'website_id' => self::VALUE_ALL == $rowData['_tier_price_website'] || $isPriceGlobal
                            ? 0
                            : $this->storeResolver->getWebsiteCodeToId(
                                $rowData['_tier_price_website']
                            ),
                        'percentage_value' => (!$fixed) ? $rowData['_tier_price_price'] : 0,
                    ];
                    $tierPrices = array_merge($tierPrices, $this->getTierPrices($rowData, $rowSku));
                } else {
                    $tierPrices += $this->getTierPrices($rowData, $rowSku);
                }
                if (!$this->validateRow($rowData, $rowNum)) {
                    $this->addLogWriteln(__('product with sku: %1 is not valided', $sku), $this->output, 'info');
                    unset($nextBunch[$rowNum]);
                    continue;
                }
                // Media gallery phase
                if ($this->publisher && isset($this->_parameters['deferred_images']) &&
                    $this->_parameters['deferred_images']) {
                    $this->importImage->addMediaGalleryRows(
                        $rowData,
                        $mediaGallery,
                        $existingImages,
                        $uploadedImages,
                        $rowNum
                    );
                } else {
                    $this->importImageProcessor->setConfig($this->_parameters);
                    $this->processMediaGalleryRows(
                        $rowData,
                        $mediaGallery,
                        $existingImages,
                        $uploadedImages,
                        $rowNum
                    );
                }

                if (!$productType === null) {
                    $previousType = $productType;
                }
                $prevAttributeSet = null;
                if (isset($rowData[self::COL_ATTR_SET])) {
                    $prevAttributeSet = $rowData[self::COL_ATTR_SET];
                }
                if (self::SCOPE_NULL == $rowScope) {
                    // for multiselect attributes only
                    if (!$prevAttributeSet === null) {
                        $rowData[self::COL_ATTR_SET] = $prevAttributeSet;
                    }
                    if ($productType === null && !$previousType === null) {
                        $productType = $previousType;
                    }
                    if ($productType === null) {
                        continue;
                    }
                }
                if (!$productType) {
                    $tempProduct = $this->skuProcessor->getNewSku($checkSku);
                    if (isset($tempProduct['type_id'])) {
                        $productType = $tempProduct['type_id'];
                    }
                }
                if ($productType) {
                    $rowScope = empty($rowData[self::COL_STORE]) ? self::SCOPE_DEFAULT : self::SCOPE_STORE;
                    $rowStore = (self::SCOPE_STORE == $rowScope)
                        ? $this->storeResolver->getStoreCodeToId($rowData[self::COL_STORE])
                        : 0;
                    $productTypeModel = $this->_productTypeModels[$productType];

                    if (!empty($rowData['tax_class_name'])) {
                        $rowData['tax_class_name'] = $this->getCurrentTaxClass($rowData['tax_class_name']);
                        $rowData['tax_class_id'] =
                            $this->taxClassProcessor->upsertTaxClass($rowData['tax_class_name'], $productTypeModel);
                    }

                    if ($this->getBehavior() == Import::BEHAVIOR_APPEND ||
                        empty($this->getCorrectSkuAsPerLength($rowData))) {
                        if (isset($this->_parameters['clear_attribute_value'])
                            && $this->_parameters['clear_attribute_value'] == 0) {
                            $rowData = $productTypeModel->clearEmptyData($rowData);
                        }
                    }

                    if (isset($this->_parameters['clear_attribute_value'])
                        && $this->_parameters['clear_attribute_value'] == 1
                    ) {
                        $rowData[self::COL_STORE] = null;
                    }
                    $rowData = $productTypeModel->prepareAttributesWithDefaultValueForSave(
                        $rowData,
                        !isset($this->_oldSku[$checkSku])
                    );
                    //google translation data
                    if ($this->translator->isTranslatorSet()) {
                        $translateAttributes = $this->_parameters['translate_attributes'] ?? [];
                        $translateStore = (int)($this->_parameters['translate_store_ids'] ?? 0);
                    }

                    foreach ($rowData as $attrCode => $attrValue) {
                        $attribute = $this->retrieveAttributeByCode($attrCode);
                        if ('multiselect' != $attribute->getFrontendInput() && self::SCOPE_NULL == $rowScope) {
                            // skip attribute processing for SCOPE_NULL rows
                            continue;
                        }
                        $attrId = $attribute->getId();
                        $backModel = $attribute->getBackendModel();
                        $attrTable = $attribute->getBackend()->getTable();
                        $storeIds = [0];

                        if ('datetime' == $attribute->getBackendType()
                            && (
                                in_array($attribute->getAttributeCode(), $this->dateAttrCodes)
                                || $attribute->getIsUserDefined()
                            )
                        ) {
                            $attrValue = $this->dateTime->formatDate($attrValue, false);
                        } elseif ('datetime' == $attribute->getBackendType() && strtotime($attrValue)) {
                            $attrValue = gmdate(
                                'Y-m-d H:i:s',
                                $this->_localeDate->date($attrValue)->getTimestamp()
                            );
                        }

                        $defaultValue = $this->getDefaultAttrValue($attribute, $rowSku);
                        $storeValue = $this->getDefaultAttrValue($attribute, $rowSku, $rowStore);

                        if (!isset($this->adminAttributeValue[$rowSku])) {
                            $this->adminAttributeValue = [$rowSku => []];
                        }

                        if (false === $defaultValue && $rowStore == 0) {
                            $this->adminAttributeValue[$rowSku][$attrCode] = $attrValue;
                        }

                        /*
                         * If storeValue exists and the default value is same as new value then remove it
                         */
                        if ($storeValue && $defaultValue === (string)$attrValue && $rowStore > 0) {
                            $this->_deleteStoreAttributeValue($attribute, $rowSku, $rowStore);
                        }
                        if ($this->translator->isTranslatorSet()) {
                            if (!empty($translateAttributes)
                                && !empty($translateStore)
                                && !isset($attributes[$attrTable][$rowSku][$attrId][$translateStore])
                                && in_array($attrCode, $translateAttributes, true)
                            ) {
                                $storeValue = $this->translator
                                    ->translateAttributeValue($attrValue, $attrCode, $translateStore);
                                $attributes[$attrTable][$rowSku][$attrId][$translateStore] = $storeValue;
                            } elseif (isset($translateStore) && $translateStore > 0) {
                                $this->_deleteStoreAttributeValue($attribute, $rowSku, $translateStore);
                            }
                        }
                        if ($defaultValue && ($defaultValue === (string)$attrValue)) {
                            continue;
                        }

                        $adminValue = $this->adminAttributeValue[$rowSku][$attrCode] ?? false;
                        if (false !== $adminValue && $adminValue === $attrValue && $rowStore > 0) {
                            continue;
                        }

                        if (self::SCOPE_STORE == $rowScope) {
                            if (self::SCOPE_WEBSITE == $attribute->getIsGlobal()) {
                                // check website defaults already set
                                if (!isset($attributes[$attrTable][$rowSku][$attrId][$rowStore])) {
                                    $storeIds = $this->storeResolver->getStoreIdToWebsiteStoreIds($rowStore);
                                }
                            } elseif (self::SCOPE_STORE == $attribute->getIsGlobal() || $attrCode == 'price') {
                                $storeIds = [$rowStore];
                            }

                            if (!isset($this->_oldSku[$checkSku])) {
                                $storeIds[] = 0;
                            }
                        }
                        $storeIds = array_unique($storeIds);
                        sort($storeIds);

                        foreach ($storeIds as $storeId) {
                            if (!isset($attributes[$attrTable][$rowSku][$attrId][$storeId])) {
                                $attributes[$attrTable][$rowSku][$attrId][$storeId] = $attrValue;
                            }
                        }
                        // restore 'backend_model' to avoid 'default' setting
                        $attribute->setBackendModel($backModel);
                    }

                    $time = explode(" ", microtime());
                    $endTime = $time[0] + $time[1];
                    $totalTime = $endTime - $startTime;
                    $totalTime = round($totalTime, 5);
                    $this->addLogWriteln(__('product with sku: %1 .... %2s', $sku, $totalTime), $this->output, 'info');
                }
            }
            if (method_exists($this, '_saveProductEntity')) {
                $this->_saveProductEntity(
                    $entityRowsIn,
                    $entityRowsUp
                );
            } else {
                $this->saveProductEntity(
                    $entityRowsIn,
                    $entityRowsUp
                );
            }

            $isCached = $this->_parameters['cache_products'] ?? false;
            if ($isCached) {
                $this->saveProductsCache($entityRowsIn, $entityRowsUp);
            }
            $this->afterSaveNewEntities($entityRowsIn);
            $this->addLogWriteln(__('Imported: %1 rows', count($entityRowsIn)), $this->output, 'info');
            $this->addLogWriteln(__('Updated: %1 rows', count($entityRowsUp)), $this->output, 'info');
            $this->_saveProductWebsites(
                $this->websitesCache
            )->_saveProductCategories(
                $this->categoriesCache
            )->_saveProductTierPrices(
                $tierPrices
            );
            if ($this->publisher && isset($this->_parameters['deferred_images']) &&
                $this->_parameters['deferred_images']) {
                $this->importImage->publishBranch();
            } else {
                $this->_saveMediaGallery($mediaGallery);
            }
            $this->_saveProductAttributes($attributes);
            $this->_saveProductCategoriesPosition($this->categoryProductPosition);

            $this->_eventManager->dispatch(
                'catalog_product_import_bunch_save_after',
                ['adapter' => $this, 'bunch' => $nextBunch]
            );
        }
        if (!empty($configurableData)) {
            $this->saveConfigurationVariations($configurableData, $existingImages);
        }

        $this->cache->clean([ImportProductCache::BUFF_CACHE]);
        return $this;
    }

    /**
     * @param $inRows
     * @param $upRows
     * @return $this
     */
    protected function saveProductsCache($inRows, $upRows)
    {
        $entityLinkField = $this->getProductEntityLinkField();
        if (!$this->originalImportRows) {
            $this->originalImportRows = $this->cache->load(sha1(ImportProductCache::BUFF_CACHE));
            $this->originalImportRows = $this->getSerializer()->unserialize($this->originalImportRows) ?? [];
        }

        foreach ($inRows as $row) {
            $id = $this->_oldSku[strtolower($row['sku'])]['entity_id'];
            foreach ($this->originalImportRows[strtolower($row['sku'])] as $item) {
                $this->cache->save(
                    1,
                    $item,
                    [ImportProductCache::CACHE_TAG . '_' . $id, ImportProductCache::CACHE_TAG]
                );
            }
        }

        $skuById = array_combine(array_column($this->_oldSku, 'entity_id'), array_keys($this->_oldSku));
        $productIdByRowId = [];
        if ($upRows) {
            $productIdByRowId = isset(current($upRows)['entity_id']) ? [] : $this->getProductIdByRowId($upRows);
        }
        foreach ($upRows as $row) {
            $id = $productIdByRowId ? $productIdByRowId[$row[$entityLinkField]] : $row['entity_id'];
            $sku = $skuById[$id];
            foreach ($this->originalImportRows[$sku] as $item) {
                $this->cache->save(
                    1,
                    $item,
                    [ImportProductCache::CACHE_TAG . '_' . $id, ImportProductCache::CACHE_TAG]
                );
            }
        }

        return $this;
    }

    /**
     * @param $rows
     * @return array
     */
    protected function getProductIdByRowId($rows)
    {
        $select = $this->_connection->select()->from(
            $this->getResource()->getTable('catalog_product_entity'),
            ['row_id', 'entity_id']
        )->where(
            'row_id IN(?)',
            array_column($rows, 'row_id')
        );

        return $this->getProductIdByRowId = $this->_connection->fetchPairs($select);
    }

    /**
     * @param $className
     * @return string
     */
    protected function getCurrentTaxClass($className)
    {
        if (!$this->classTaxNames) {
            $select = $this->_connection->select()->from(
                $this->getResource()->getTable('tax_class'),
                'class_name'
            )->where(
                'class_type = ?',
                ClassModel::TAX_CLASS_TYPE_PRODUCT
            );

            $result = [];
            foreach ($this->_connection->fetchCol($select) as $item) {
                $result [strtolower($item)] = $item;
            }
            $this->classTaxNames = $result;
        }

        $key = strtolower($className);
        return key_exists($key, $this->classTaxNames) ? $this->classTaxNames[$key] : $className;
    }

    /**
     * @param string $fileName
     * @param bool $renameFileOff
     * @param array $existingUpload
     *
     * @return string
     */
    protected function uploadMediaFiles($fileName, $renameFileOff = false, $existingUpload = [])
    {
        return $this->importImageProcessor->uploadMediaFiles($fileName, $renameFileOff, $existingUpload);
    }

    /**
     * @param $entityRowsIn
     * @throws Exception
     */
    protected function afterSaveNewEntities($entityRowsIn)
    {
        if ($entityRowsIn) {
            $entityTable = $this->_resourceFactory->create()->getEntityTable();

            $select = $this->_connection->select()->from(
                $entityTable,
                array_merge($this->getNewSkuFieldsForSelect(), $this->getOldSkuFieldsForSelect())
            )->where(
                $this->_connection->quoteInto('sku IN (?)', array_keys($entityRowsIn))
            );
            $newProducts = $this->_connection->fetchAll($select);
            foreach ($newProducts as $data) {
                $this->_optionEntity->addNewSkuToId($data['entity_id'], $data['sku']);
            }
        }
    }

    /**
     * Return additional data, needed to select.
     * @return array
     */
    private function getOldSkuFieldsForSelect()
    {
        return ['type_id', 'attribute_set_id'];
    }

    /**
     * Get product entity identifier field
     *
     * @return string
     * @throws Exception
     */
    private function getProductIdentifierField()
    {
        if (!$this->productEntityIdentifierField) {
            $this->productEntityIdentifierField = $this->getMetadataPool()
                ->getMetadata(ProductInterface::class)
                ->getIdentifierField();
        }
        return $this->productEntityIdentifierField;
    }

    /**
     * Get new SKU fields for select
     *
     * @return array
     * @throws Exception
     */
    private function getNewSkuFieldsForSelect()
    {
        $fields = ['sku', $this->getProductEntityLinkField()];
        if ($this->getProductEntityLinkField() != $this->getProductIdentifierField()) {
            $fields[] = $this->getProductIdentifierField();
        }
        return $fields;
    }

    /**
     * @param array $categoriesData
     * @param null $productId
     * @param bool $config
     *
     * @return $this|MagentoProduct
     */
    protected function _saveProductCategories(array $categoriesData, $productId = null, $config = false)
    {
        static $tableName = null;
        $removeCategories = $this->_parameters['remove_product_categories'] ?? 0;
        if ($removeCategories || $productId) {
            if (!$tableName) {
                $tableName = $this->_resourceFactory->create()->getProductCategoryTable();
            }
            if ($categoriesData) {
                $categoriesIn = [];
                $delProductId = [];

                foreach ($categoriesData as $productSku => $categories) {
                    if (!$config) {
                        $productId = $this->skuProcessor->getNewSku($productSku)['entity_id'];
                    }
                    $delProductId[] = $productId;
                    if ($this->onlyUpdate
                        && (int)$removeCategories > 0
                        && empty($categories)
                    ) {
                        $this->addLogWriteln(
                            __('Product %1 Categories Cannot be Cleared', $productSku),
                            $this->output,
                            'info'
                        );
                        continue;
                    }
                    $cat = [];
                    foreach ($categories as $category => $delete) {
                        if ($delete === true) {
                            $cat[$category] = true;
                        }
                    }

                    foreach (array_keys($cat) as $categoryId) {
                        $position = 1;
                        try {
                            $positions = $this->getCategoryPosition($categoryId);
                            $existingPositions = [];
                            foreach ($positions as $position) {
                                $existingPositions[] = $position['position'];
                            }
                            if (!in_array(1, $existingPositions)) {
                                $position = 1;
                            } elseif ($maxPos = max($existingPositions)) {
                                $position = $maxPos + 1;
                            }
                        } catch (Exception $exception) {
                            $position = 1;
                        }
                        $categoriesIn[] = [
                            'product_id' => $productId,
                            'category_id' => $categoryId,
                            'position' => $position
                        ];
                    }
                }
                if ($removeCategories || $config) {
                    $this->_connection->delete(
                        $tableName,
                        $this->_connection->quoteInto('product_id IN (?)', $delProductId)
                    );
                }
                if ($categoriesIn) {
                    $this->_connection->insertOnDuplicate($tableName, $categoriesIn, ['product_id', 'category_id']);
                }
            }
            return $this;
        }
        return parent::_saveProductCategories($categoriesData);
    }

    /**
     * @param array $categoryProductPosition
     * @return $this
     */
    protected function _saveProductCategoriesPosition(array $categoryProductPosition)
    {
        static $tableName = null;

        if (!$tableName) {
            $tableName = $this->_resource->getTable('catalog_category_product');
        }

        $positionsIn = [];
        foreach ($categoryProductPosition as $sku => $categories) {
            $productId = $this->skuProcessor->getNewSku($sku)['entity_id'];
            foreach ($categories as $categoryId => $position) {
                $positionsIn[] = ['product_id' => $productId, 'category_id' => $categoryId, 'position' => $position];
            }
        }

        if ($positionsIn) {
            $this->_connection->insertOnDuplicate($tableName, $positionsIn, ['position']);
        }

        return $this;
    }

    /**
     * Retrieve default attribute value (where store_id = 0)
     *
     * @param AbstractAttribute $attribute
     * @param string $sku
     * @param int $storeId
     * @return bool|string
     * @throws Exception
     */
    protected function getDefaultAttrValue(AbstractAttribute $attribute, $sku, $storeId = 0)
    {
        if (!isset($this->_oldSku[strtolower($sku)])) {
            return false;
        }

        $linkField = $this->getProductEntityLinkField();
        $linkId = $this->_oldSku[strtolower($sku)][$linkField];

        $bind = [
            'attribute_id' => $attribute->getId(),
            'store_id' => $storeId,
            $linkField => $linkId,
        ];

        $select = $this->_connection->select()
            ->from($attribute->getBackend()->getTable(), 'value')
            ->where('attribute_id = :attribute_id')
            ->where('store_id = :store_id')
            ->where($linkField . ' = :' . $linkField);

        return $this->_connection->fetchOne($select, $bind);
    }

    /**
     * @param AbstractAttribute $attribute
     * @param string $sku
     * @param int $storeId
     * @return bool|string
     * @throws Exception
     */
    protected function _deleteStoreAttributeValue(AbstractAttribute $attribute, $sku, $storeId = 0)
    {
        if (!isset($this->_oldSku[strtolower($sku)])) {
            return false;
        }

        $linkField = $this->getProductEntityLinkField();
        $linkId = $this->_oldSku[strtolower($sku)][$linkField];

        $deleteCondition[] = $this->_connection->quoteInto(
            'attribute_id = ?',
            $attribute->getId()
        );
        $deleteCondition[] = $this->_connection->quoteInto(
            'store_id = ?',
            $storeId
        );
        $deleteCondition[] = $this->_connection->quoteInto(
            $linkField . ' = ?',
            $linkId
        );

        $deleteCondition = implode(' AND ', $deleteCondition);

        return $this->_connection->delete($attribute->getBackend()->getTable(), $deleteCondition);
    }

    /**
     * Init media gallery resources.
     *
     * @return void
     */
    public function initMediaGalleryResources()
    {
        if (null == $this->mediaGalleryTableName) {
            $this->productEntityTableName = $this->getResource()->getTable('catalog_product_entity');
            $this->mediaGalleryTableName = $this->getResource()->getTable('catalog_product_entity_media_gallery');
            $this->mediaGalleryValueTableName = $this->getResource()->getTable(
                'catalog_product_entity_media_gallery_value'
            );
            $this->mediaGalleryEntityToValueTableName = $this->getResource()->getTable(
                'catalog_product_entity_media_gallery_value_to_entity'
            );
        }
    }

    /**
     * @param $newMediaValues
     * @return $this
     */
    protected function removeExistingImages($newMediaValues)
    {
        $this->importImageProcessor->removeExistingImages($newMediaValues);
        return $this;
    }

    /**
     * @param string $sku
     * @return $this
     */
    protected function removeRelatedProducts($sku)
    {
        if (!isset($this->_oldSku[strtolower($sku)])) {
            return $this;
        }
        try {
            $entityLinkField = $this->getProductEntityLinkField();
            $productId = $this->_oldSku[strtolower($sku)][$entityLinkField];
            $linkTypeId = ProductLink::LINK_TYPE_RELATED;
            $linkTable = $this->getResource()->getTable('catalog_product_link');
            $this->_connection->delete(
                $linkTable,
                ['product_id=' . $productId, 'link_type_id=' . $linkTypeId]
            );
        } catch (Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->output, 'error');
        }

        return $this;
    }

    /**
     * @param $sku
     *
     * @return $this
     */
    protected function removeCrosssellProducts($sku)
    {
        if (!isset($this->_oldSku[strtolower($sku)])) {
            return $this;
        }
        try {
            $entityLinkField = $this->getProductEntityLinkField();
            $productId = $this->_oldSku[strtolower($sku)][$entityLinkField];
            $linkTypeId = ProductLink::LINK_TYPE_CROSSSELL;
            $linkTable = $this->getResource()->getTable('catalog_product_link');
            $this->_connection->delete(
                $linkTable,
                ['product_id=' . $productId, 'link_type_id=' . $linkTypeId]
            );
        } catch (Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->output, 'error');
        }

        return $this;
    }

    /**
     * @param $sku
     *
     * @return $this
     */
    protected function removeUpsellProducts($sku)
    {
        if (!isset($this->_oldSku[strtolower($sku)])) {
            return $this;
        }
        try {
            $entityLinkField = $this->getProductEntityLinkField();
            $productId = $this->_oldSku[strtolower($sku)][$entityLinkField];
            $linkTypeId = ProductLink::LINK_TYPE_UPSELL;
            $linkTable = $this->getResource()->getTable('catalog_product_link');
            $this->_connection->delete(
                $linkTable,
                ['product_id=' . $productId, 'link_type_id=' . $linkTypeId]
            );
        } catch (Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->output, 'error');
        }

        return $this;
    }

    /**
     * Initialize source type model
     *
     * @param $type
     *
     * @throws LocalizedException
     */
    protected function _initSourceType($type)
    {
        if (!$this->sourceType) {
            $this->sourceType = $this->additional->getSourceModelByType($type);
            $this->sourceType->setData($this->_parameters);
        }
    }

    /**
     * Import images via initialized source type
     *
     * @param $bunch
     *
     * @return mixed
     */
    protected function prepareImagesFromSource($bunch)
    {
        foreach ($bunch as $rowNum => &$rowData) {
            $rowData = $this->customFieldsMapping($rowData);
            $this->addLogWriteln(
                __('Downloading Image From Source for product sku %1', $this->getCorrectSkuAsPerLength($rowData)),
                $this->getOutput(),
                'info'
            );
            foreach ($this->_imagesArrayKeys as $image) {
                if (empty($rowData[$image])) {
                    continue;
                }
                $dispersionPath = \Magento\Framework\File\Uploader::getDispretionPath($rowData[$image]);
                $importImages = explode($this->getMultipleValueSeparator(), $rowData[$image]);
                $imageArr = [];
                foreach ($importImages as $importImage) {
                    $imageSting = mb_strtolower(
                        $dispersionPath . '/' . preg_replace('/[^a-z0-9\._-]+/i', '', $importImage)
                    );
                    if ($this->sourceType) {
                        if ($this->sourceType->getCode() === 'rest') {
                            $sourceImport = $this->sourceType->importImage($importImage, $imageSting);
                            $imageArr[] = $this->sourceType->getCode() . $sourceImport[1];
                        } else {
                            $this->sourceType->importImage($importImage, $imageSting);
                        }
                    }
                    if ($this->sourceType->getCode() !== 'rest') {
                        $imageArr[] = $this->sourceType->getCode() . $imageSting;
                    }
                }
                $rowData[$image] = implode($this->getMultipleValueSeparator(), $imageArr);
            }
        }

        return $bunch;
    }

    /**
     * @param array $rowData
     * @param null $storeIds
     * @return array
     * @throws Exception
     */
    protected function generateUrlKey(array $rowData, $storeIds = null)
    {
        $productEntityLinkField = $this->getProductEntityLinkField();
        $sku = $this->getCorrectSkuAsPerLength($rowData);
        $urlKey = $rowData[self::URL_KEY] ?? '';
        $name = $rowData[self::COL_NAME] ?? '';
        if ($this->isSkuExist($sku)) {
            $exiting = $this->getExistingSku($sku);
            if (!$urlKey) {
                $attr = $this->retrieveAttributeByCode(self::URL_KEY);
                $select = $this->getConnection()->select()
                    ->from($attr->getBackendTable(), ['value'])
                    ->where($productEntityLinkField . ' = (?)', $exiting['entity_id'])
                    ->where('attribute_id = (?)', $attr->getAttributeId());
                $urlKey = $this->getConnection()->fetchOne($select);
            }
            if (!$name) {
                $attr = $this->retrieveAttributeByCode(self::COL_NAME);
                $select = $this->getConnection()->select()
                    ->from($attr->getBackendTable(), ['value'])
                    ->where($productEntityLinkField . ' = (?)', $exiting['entity_id'])
                    ->where('attribute_id = (?)', $attr->getAttributeId());
                $name = $this->getConnection()->fetchOne($select);
                if (!$urlKey) {
                    $urlKey = $name;
                }
            }
        } else {
            $urlKey = isset($rowData[self::URL_KEY])
                ? $urlKey
                : $name;
        }
        if ($storeIds === null) {
            $storeIds = $this->getStoreIds();
        }
        $urlKey = ($urlKey != '') ?
            $this->productUrl->formatUrlKey($urlKey)
            : $this->productUrl->formatUrlKey($name);
        $isDuplicate = $this->isDuplicateUrlKey($urlKey, $sku, $storeIds);
        if ($isDuplicate || $this->urlKeyManager->isUrlKeyExist($sku, $urlKey)) {
            $urlKey = $this->productUrl->formatUrlKey(
                $name . '-' . $sku
            );
        }
        $rowData[self::URL_KEY] = $urlKey;
        $this->urlKeyManager->addUrlKeys($sku, $urlKey);
        return $rowData;
    }

    /**
     * Custom fields mapping for changed purposes of fields and field names.
     *
     * @param array $rowData
     *
     * @return array
     */
    public function customFieldsMapping($rowData)
    {
        $rowData = $this->attributeValuesMapping($rowData);

        foreach ($this->_fieldsMap as $systemFieldName => $fileFieldName) {
            if (array_key_exists($fileFieldName, $rowData) && !isset($rowData[$systemFieldName])) {
                $rowData[$systemFieldName] = $rowData[$fileFieldName];
            }
        }
        // restore data for configurable field when it is already used in Map Attributes section
        $configField = $this->_parameters['configurable_field'];
        if ($configField && !isset($rowData[$configField])) {
            $configKey = array_search($configField, $this->_fieldsMap);
            if ($configKey !== false) {
                $rowData[$configField] = $rowData[$configKey];
            }
        }
        //
        $rowData = $this->_parseAdditionalAttributes($rowData);
        $rowData = $this->setStockUseConfigFieldsValues($rowData);

        if (array_key_exists('status', $rowData)
            && $rowData['status'] != Status::STATUS_ENABLED
        ) {
            if ($rowData['status'] == 'yes') {
                $rowData['status'] = Status::STATUS_ENABLED;
            } elseif (!empty($rowData['status']) || $this->getRowScope($rowData) == self::SCOPE_DEFAULT) {
                $rowData['status'] = Status::STATUS_DISABLED;
            }
        }
        $imageImportPath = $this->_parameters[Import::FIELD_NAME_IMG_FILE_DIR] ?? '';
        if (preg_match('/\bhttps?:\/\//i', $imageImportPath, $matches)) {
            foreach ($this->_imagesArrayKeys as $image) {
                if (empty($rowData[$image]) && !isset($rowData[$image])) {
                    continue;
                }
                if ($image === 'additional_images') {
                    $addImage = [];
                    foreach (explode($this->getMultipleValueSeparator(), $rowData[$image]) as $rowImage) {
                        $addImage[] = $imageImportPath . '/' . $rowImage;
                    }
                    $rowData[$image] = implode($this->getMultipleValueSeparator(), $addImage);
                } else {
                    $rowData[$image] = $imageImportPath . '/' . $rowData[$image];
                }
            }
        }

        foreach ($this->_imagesArrayKeys as $image) {
            if ($image != '_media_image') {
                if (isset($rowData[$image])) {
                    $rowData[$image] = trim($rowData[$image]);
                }
            }
        }

        return $rowData;
    }

    /**
     * Parse attributes names and values string to array.
     *
     * @param array $rowData
     *
     * @return array
     */
    private function _parseAdditionalAttributes($rowData)
    {
        if (empty($rowData['additional_attributes'])) {
            return $rowData;
        }
        try {
            $source = $this->_getSource();
        } catch (Exception $e) {
            $source = null;
        }
        $valuePairs = explode(
            $this->getMultipleValueSeparator(),
            $rowData['additional_attributes']
        );
        foreach ($valuePairs as $valuePair) {
            $separatorPosition = strpos($valuePair, self::PAIR_NAME_VALUE_SEPARATOR);
            if ($separatorPosition !== false) {
                $key = substr($valuePair, 0, $separatorPosition);
                $value = substr(
                    $valuePair,
                    $separatorPosition + strlen(self::PAIR_NAME_VALUE_SEPARATOR)
                );
                if ($source !== null) {
                    $key = $source->changeField($key);
                }
                $multiLineSeparator = strpos($value, self::PSEUDO_MULTI_LINE_SEPARATOR);
                if ($multiLineSeparator !== false) {
                    $attribute = $this->retrieveAttributeByCode($key);
                    if ($attribute
                        && $attribute->getBackendType() !== 'varchar'
                        && $attribute->getBackendType() !== 'text'
                        && !$this->verifySwatchString($value)
                    ) {
                        $value = implode(
                            $this->getMultipleValueSeparator(),
                            explode(
                                self::PSEUDO_MULTI_LINE_SEPARATOR,
                                $value
                            )
                        );
                    }
                }
                $rowData[$key] = $value === false ? '' : $value;
            }
        }

        return $rowData;
    }

    /**
     * @param string $value
     * @return bool
     */
    private function verifySwatchString(string $value)
    {
        return (strpos($value, 'type=') !== false && strpos($value, 'value=') !== false)
            ? true
            : false;
    }

    /**
     * Set values in use_config_ fields.
     *
     * @param array $rowData
     *
     * @return array
     */
    private function setStockUseConfigFieldsValues($rowData)
    {
        $useConfigFields = [];
        foreach ($rowData as $key => $value) {
            if (isset($this->defaultStockData[$key])
                && isset($this->defaultStockData[self::INVENTORY_USE_CONFIG_PREFIX . $key])
                && !empty($value)
            ) {
                $useConfigFields[self::INVENTORY_USE_CONFIG_PREFIX . $key] =
                    ($value == self::INVENTORY_USE_CONFIG) ? 1 : 0;
            }
        }
        if (!empty($useConfigFields)) {
            $rowData = array_merge($rowData, $useConfigFields);
        }

        return $rowData;
    }

    /**
     * Replace attribute values according map
     *
     * @param array $rowData
     *
     * @return array
     */
    protected function attributeValuesMapping($rowData)
    {
        /** @var \Firebear\ImportExport\Model\ResourceModel\Job\Collection $collection */
        $collection = $this->importCollection->create();
        $collection->addFieldToFilter($collection->getIdFieldName(), $this->_parameters['job_id']);

        /** @var Job $job */
        foreach ($collection as $job) {
            $map = Serializer::unserialize($job->getMapping());

            foreach ($map as $item) {
                if (isset($item['source_data_attribute_value_system']) &&
                    isset($item['source_data_attribute_value_import'])
                ) {
                    foreach ($rowData as $key => $value) {
                        if (!is_array($value) && (trim($value) == trim($item['source_data_attribute_value_import']))) {
                            $rowData[$key] = $item['source_data_attribute_value_system'];
                        }
                    }
                }
            }
        }

        return $rowData;
    }

    /**
     * @param $rowData
     * @return array
     */
    protected function categoriesMapping($rowData)
    {
        $categories_separator = $this->_parameters['categories_separator'];
        $explodeImportedCategoriesItems = explode($categories_separator, $rowData[self::COL_CATEGORY]);

        $explodeImportedCategoriesPositionItems = [];
        $importedCategoriesPositionItems = (!empty($rowData[self::COL_CATEGORY . '_position']))
            ? explode($categories_separator, $rowData[self::COL_CATEGORY . '_position'])
            : [];
        foreach ($importedCategoriesPositionItems as $index => $positionItem) {
            if (empty($positionItem)) {
                continue;
            }
            $categoriesPositionValue = explode(self::PAIR_NAME_VALUE_SEPARATOR, $positionItem);
            $explodeImportedCategoriesPositionItems[$categoriesPositionValue[0]] = [
                $index,
                $categoriesPositionValue[1] ?? 0
            ];
        }

        $connection = $this->_connection;
        $resource = $this->getResource();
        $select = $connection->select()->from(
            [
                'main' => $resource->getTable('firebear_import_jobs'),
            ],
            ['mapping']
        )->where('entity_id=?', $this->_parameters['job_id']);
        $maps = $this->_connection->fetchAll(
            $select
        );
        foreach ($maps as $map) {
            $newCategoriesMapItems = Serializer::unserialize($map['mapping']);
            foreach ($newCategoriesMapItems as $newCategoriesMapItem) {
                foreach ($explodeImportedCategoriesItems as &$explodeImportedCategoriesItem) {
                    if (isset($newCategoriesMapItem['source_category_data_import']) &&
                        trim($explodeImportedCategoriesItem) == $newCategoriesMapItem['source_category_data_import']
                    ) {
                        $explodeImportedCategoriesItem = $newCategoriesMapItem['source_category_data_new'];
                        $this->setIsRowCategoryMapped(true);

                        if (isset($explodeImportedCategoriesPositionItems[$explodeImportedCategoriesItem])) {
                            $index = $explodeImportedCategoriesPositionItems[$explodeImportedCategoriesItem][0];
                            $position = $explodeImportedCategoriesPositionItems[$explodeImportedCategoriesItem][1];
                            $importedCategoriesPositionItems[$index] = implode(
                                self::PAIR_NAME_VALUE_SEPARATOR,
                                [$newCategoriesMapItem['source_category_data_new'], $position]
                            );
                        }
                    }
                }
            }
        }
        return [
            self::COL_CATEGORY => implode($categories_separator, $explodeImportedCategoriesItems),
            self::COL_CATEGORY . '_position' => implode($categories_separator, $importedCategoriesPositionItems)
        ];
    }

    /**
     * @param MagentoProduct\Type\AbstractType $productTypeModel
     * @param array $rowData
     * @return array
     * @throws LocalizedException
     */
    public function createAttributeValues(
        MagentoProduct\Type\AbstractType $productTypeModel,
        array $rowData
    ) {
        $options = [];
        if (isset($rowData[self::COL_ATTR_SET])) {
            $attributeSet = $rowData[self::COL_ATTR_SET];
            foreach ($rowData as $attrCode => $attrValue) {
                /**
                 * Add attribute to set & set's group
                 */
                if (preg_match('/^(attribute\|).+/', $attrCode)) {
                    $columnData = explode('|', $attrCode);
                    $columnData = $this->prepareAttributeData($columnData);
                    // might be already inside additional_attributes
                    if (isset($rowData[$columnData['attribute_code']])) {
                        unset($rowData[$attrCode]);
                        continue;
                    } else {
                        $rowData[$columnData['attribute_code']] = $rowData[$attrCode];
                        unset($rowData[$attrCode]);
                        $attrCode = $columnData['attribute_code'];
                    }
                }

                /**
                 * Prepare new values
                 */
                $attrParams = $productTypeModel->retrieveAttribute($attrCode, $attributeSet);
                if (!empty($attrParams)) {
                    if (!$attrParams['is_static'] && isset($rowData[$attrCode]) && trim($rowData[$attrCode]) !== '') {
                        switch ($attrParams['type']) {
                            case 'select':
                                $swatchOptionData = [];
                                $swatchOptions = [];
                                /** @var \Magento\Catalog\Model\ResourceModel\Eav\Attribute\Interceptor $attribute */
                                $attribute = $this->retrieveAttributeByCode($attrCode);
                                if ($this->swatchesHelperData->isVisualSwatch($attribute)) {
                                    $swatchOptionData = $this->prepareSwatchOptionData($rowData[$attrCode]);
                                    $swatchOptions = $this->getSwatchesByOptionsId(
                                        $attrParams['options'],
                                        $attrParams['id']
                                    );
                                }
                                if ($this->swatchesHelperData->isTextSwatch($attribute)) {
                                    $swatchOptionData = $rowData[$attrCode];
                                    $swatchOptions = $this->getSwatchesByOptionsId(
                                        $attrParams['options'],
                                        $attrParams['id']
                                    );
                                }
                                if ((isset($attrParams['additional_data']['update_product_preview_image'])
                                        || isset($attrParams['additional_data']['use_product_image_for_swatch']))
                                    && $this->verifySwatchString($rowData[$attrCode])
                                ) {
                                    $swatchOptionData = $rowData[$attrCode];
                                    $swatchOptions = $this->getSwatchesByOptionsId(
                                        $attrParams['options'],
                                        $attrParams['id']
                                    );
                                }
                                $lowerAttrValue = strtolower($rowData[$attrCode]);
                                // no attribute option
                                $storeCode = isset($rowData[self::COL_STORE_VIEW_CODE])
                                    ? $rowData[self::COL_STORE_VIEW_CODE] : false;
                                $scopeStore = self::SCOPE_STORE == $this->getRowScope($rowData);
                                $attrOptions = $attrParams['options'];
                                if ($scopeStore && $storeCode && !empty($attrParams['options_store'][$storeCode])) {
                                    if (isset($attrOptions[$lowerAttrValue]) &&
                                        !isset($attrParams['options_store'][$storeCode][$lowerAttrValue])
                                    ) {
                                        $attrParams['options_store'][$storeCode][$lowerAttrValue] =
                                            $attrOptions[$lowerAttrValue];
                                    }
                                    $attrOptions = $attrParams['options_store'][$storeCode];
                                }

                                if (!isset($attrOptions[$lowerAttrValue])) {
                                    $options[$attrParams['id']][] = [
                                        'sort_order' => count($attrParams['options']) + 1,
                                        'value' => $rowData[$attrCode],
                                        'code' => $attrCode,
                                        'swatch_option' => $swatchOptionData,
                                    ];
                                } elseif (!empty($swatchOptionData) &&
                                    !array_key_exists($attrOptions[$lowerAttrValue], $swatchOptions)
                                ) { // no attribute swatch option
                                    $newSwatchOptions[$attrParams['id']][$attrOptions[$lowerAttrValue]] =
                                        $swatchOptionData;
                                } elseif (array_key_exists($attrOptions[$lowerAttrValue], $swatchOptions)) {
                                    // swatch attribute option exist
                                    $swatchOption = $swatchOptions[$attrOptions[$lowerAttrValue]];
                                    if ($this->swatchesHelperData->isVisualSwatch($attribute)) {
                                        // but has different value or type
                                        if (isset($attrParams['additional_data']['update_product_preview_image'])
                                            || isset($attrParams['additional_data']['use_product_image_for_swatch'])
                                            && !is_array($swatchOptionData)
                                        ) {
                                            break; //continue 2
                                        }
                                        if (!empty($diff = array_diff_assoc($swatchOptionData, $swatchOption))) {
                                            if ((array_key_exists('type', $diff) &&
                                                    $diff['type'] == Swatch::SWATCH_TYPE_VISUAL_COLOR)
                                                || (!array_key_exists('type', $diff) &&
                                                    $swatchOption['type'] == Swatch::SWATCH_TYPE_VISUAL_COLOR)
                                            ) {
                                                $this->updateSwatchOption($swatchOption, $diff);
                                            } elseif ($this->ifVisualSwatchOptionDifferent($swatchOption, $diff)) {
                                                $diff['value'] = $this->uploadVisualSwatchFile($diff['value']);
                                                $this->updateSwatchOption($swatchOption, $diff);
                                            }
                                        }
                                    }
                                }
                                break;
                            case 'multiselect':
                                $separator = $this->_parameters['_import_multiple_value_separator'] ?
                                    $this->_parameters['_import_multiple_value_separator'] :
                                    MagentoProduct::PSEUDO_MULTI_LINE_SEPARATOR;
                                $values = explode($separator, $rowData[$attrCode]);
                                foreach ($values as $value) {
                                    $value = trim($value);
                                    if (!isset($attrParams['options'][strtolower($value)])) {
                                        $options[$attrParams['id']][] = [
                                            'sort_order' => count($attrParams['options']) + 1,
                                            'value' => $value,
                                            'code' => $attrCode,
                                        ];
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

            /**
             * Create new values
             */
            if (!empty($options)) {
                $connection = $this->_connection;
                $resource = $this->getResource();
                foreach ($options as $attributeId => $optionsArray) {
                    foreach ($optionsArray as $option) {
                        /**
                         * @see \Magento\Eav\Model\ResourceModel\Entity\Attribute::_updateAttributeOption()
                         */
                        $table = $resource->getTable('eav_attribute_option');
                        $data = ['attribute_id' => $attributeId, 'sort_order' => $option['sort_order']];
                        $connection->insert($table, $data);
                        $intOptionId = $connection->lastInsertId($table);
                        /**
                         * @see \Magento\Eav\Model\ResourceModel\Entity\Attribute::_updateAttributeOptionValues()
                         */
                        $table = $resource->getTable('eav_attribute_option_value');
                        $data = ['option_id' => $intOptionId, 'store_id' => 0, 'value' => $option['value']];
                        $connection->insert($table, $data);
                        if (isset($option['swatch_option']) && !empty($option['swatch_option'])) {
                            $this->insertNewSwatchOption(
                                $connection,
                                $resource,
                                $intOptionId,
                                $option['swatch_option'],
                                $attributeId
                            );
                        }
                        foreach ($this->_productTypeModels as $productTypeModel) {
                            $productTypeModel->addAttributeOption(
                                $option['code'],
                                strtolower($option['value']),
                                $intOptionId
                            );
                        }
                    }
                }
            }
            if (!empty($newSwatchOptions)) {
                $connection = $this->_connection;
                $resource = $this->getResource();
                foreach ($newSwatchOptions as $attributeId => $swatchOption) {
                    foreach ($swatchOption as $optionId => $swatchData) {
                        $this->insertNewSwatchOption($connection, $resource, $optionId, $swatchData, $attributeId);
                    }
                }
            }
        }

        return $rowData;
    }

    /**
     * Convert attribute string syntax to array.
     *
     * @param $columnData
     *
     * @return array
     * @throws Exception
     */
    protected function prepareAttributeData($columnData)
    {
        $result = [];
        foreach ($columnData as $field) {
            $field = explode(':', $field);
            if (isset($field[1])) {
                if (preg_match('/^(frontend_label_)[0-9]+/', $field[0])) {
                    $result['frontend_label'][(int)substr($field[0], -1)] = $field[1];
                } else {
                    $result[$field[0]] = $field[1];
                }
            }
        }

        if (!empty($result)) {
            $attributeCode = isset($result['attribute_code']) ? $result['attribute_code'] : null;
            $frontendLabel = $result['frontend_label'][0];
            $attributeCode = $attributeCode ?: $this->generateAttributeCode($frontendLabel);
            $result['attribute_code'] = $attributeCode;

            $entityTypeId = $this->eavEntityFactory->create()->setType(
                \Magento\Catalog\Model\Product::ENTITY
            )->getTypeId();
            $result['entity_type_id'] = $entityTypeId;
            $result['is_user_defined'] = 1;
        }

        return $result;
    }

    /**
     * Generate code from label
     *
     * @param string $label
     * @return string
     * @throws Zend_Validate_Exception
     */
    protected function generateAttributeCode($label)
    {
        $code = substr(
            preg_replace(
                '/[^a-z_0-9]/',
                '_',
                $this->productUrl->formatUrlKey($label)
            ),
            0,
            30
        );
        $validatorAttrCode = new Zend_Validate_Regex(['pattern' => '/^[a-z][a-z_0-9]{0,29}[a-z0-9]$/']);
        if (!$validatorAttrCode->isValid($code)) {
            $code = 'attr_' . ($code ?: substr(hash("md5", time()), 0, 8));
        }

        return $code;
    }

    /**
     * Parse swatch attribute value, pulls actual attribute value and swatch options if they are there.
     *
     * @param $attributeValue string Swatch attribute value in follow format
     *     "{value}|type={1,2}|value={#FFFFFF,path/to/image.file}" where: type:1 - color type:2 - image
     *
     * @return array
     */
    protected function prepareSwatchOptionData(&$attributeValue)
    {
        $swatchOptionData = [];
        $preParsedData = explode('|', $attributeValue);
        if (count($preParsedData) > 1) {
            foreach ($preParsedData as $key => $value) {
                if ($key == 0) {
                    $attributeValue = $value; //set attributes value
                    continue;
                }
                $value = explode('=', $value);
                if (isset($value[1])) {
                    $swatchOptionData[$value[0]] = $value[1];
                }
            }
        }

        return $swatchOptionData;
    }

    /**
     * Returns Swatch option data for Attribute Option Ids
     *
     * @param array $optionIds
     * @param int $attributeId
     *
     * @return array
     */
    protected function getSwatchesByOptionsId($optionIds, $attributeId)
    {
        if (!isset($this->cachedSwatchOptions[$attributeId]) || empty($this->cachedSwatchOptions[$attributeId])) {
            $this->cachedSwatchOptions[$attributeId] = [];
            $swatchCollection = $this->swatchCollectionFactory->create();
            $swatchCollection->addFilterByOptionsIds($optionIds);
            foreach ($swatchCollection as $item) {
                $this->cachedSwatchOptions[$attributeId][$item['option_id']] = $item->getData();
            }
        }

        return $this->cachedSwatchOptions[$attributeId];
    }

    /**
     * @param int $swatchOption
     * @param array $diff
     */
    protected function updateSwatchOption($swatchOption, $diff)
    {
        $connection = $this->_connection;
        $resource = $this->getResource();
        $table = $resource->getTable('eav_attribute_option_swatch');
        if (isset($swatchOption['swatch_id'])) {
            $where = ['swatch_id=?' => (int)$swatchOption['swatch_id']];
            $connection->update($table, $diff, $where);
        }
    }

    /**
     * Checks if imported image for swatch option is different then exist one.
     *
     * @param int $swatchOption
     * @param array $diff Array of type and value that are different
     *
     * @return bool
     * @throws LocalizedException
     */
    protected function ifVisualSwatchOptionDifferent($swatchOption, $diff)
    {
        // TODO: need implement logic for unique names -
        // sometimes image name might have _1_2 endings for the same image.
        if (isset($diff['value'])) {
            $fileName = preg_replace('/[^a-z0-9\._-]+/i', '', $diff['value']);
            $dispretionPath = $this->_getUploader()->getDispretionPath($fileName);
            return !($swatchOption['value'] == $dispretionPath . '/' . $fileName);
        }
        return false;
    }

    /**
     * @return \Magento\CatalogImportExport\Model\Import\Uploader|Uploader
     * @throws LocalizedException
     */
    protected function _getUploader()
    {
        $DS = DIRECTORY_SEPARATOR;
        if ($this->_fileUploader === null) {
            $this->_fileUploader = $this->_uploaderFactory->create();
            $this->_fileUploader->init();
            $dirConfig = DirectoryList::getDefaultConfig();
            $dirAddon = $dirConfig[DirectoryList::MEDIA][DirectoryList::PATH];
            if (!empty($this->_parameters[Import::FIELD_NAME_IMG_FILE_DIR])) {
                $tmpPath = $this->_parameters[Import::FIELD_NAME_IMG_FILE_DIR];
            } else {
                $tmpPath = $dirAddon . $DS . $this->_mediaDirectory->getRelativePath('import');
            }
            if (preg_match('/\bhttps?:\/\//i', $tmpPath, $matches)) {
                $tmpPath = $dirAddon . $DS . $this->_mediaDirectory->getRelativePath('import');
            }
            if (!$this->_fileUploader->setTmpDir($tmpPath)) {
                $this->addLogWriteln(__('File directory \'%1\' is not readable.', $tmpPath), $this->output, 'info');
                $this->addRowError(
                    __('File directory \'%1\' is not readable.', $tmpPath),
                    null,
                    null,
                    null,
                    ProcessingError::ERROR_LEVEL_NOT_CRITICAL
                );
                throw new LocalizedException(
                    __('File directory \'%1\' is not readable.', $tmpPath)
                );
            }
            $destinationDir = "catalog/product";
            $destinationPath = $dirAddon . $DS . $this->_mediaDirectory->getRelativePath($destinationDir);

            $this->_mediaDirectory->create($destinationPath);
            if (!$this->_fileUploader->setDestDir($destinationPath)) {
                $this->addRowError(
                    __('File directory \'%1\' is not writable.', $destinationPath),
                    null,
                    null,
                    null,
                    ProcessingError::ERROR_LEVEL_NOT_CRITICAL
                );
                throw new LocalizedException(
                    __('File directory \'%1\' is not writable.', $destinationPath)
                );
            }
        }

        if ($this->_fileUploader) {
            $this->_fileUploader->setEntity($this);
            $this->_fileUploader->setOutput($this->getOutput());
        }

        return $this->_fileUploader;
    }

    /**
     * Uploads Image for Image Swatch option
     *
     * @param string $swatchVisualFile
     * @return string
     * @throws LocalizedException
     */
    protected function uploadVisualSwatchFile($swatchVisualFile)
    {
        $config = $this->mediaConfig;
        $uploader = $this->_getUploader();
        $newFile = '';
        $dirConfig = DirectoryList::getDefaultConfig();
        $mediaRelativePath = $dirConfig[DirectoryList::MEDIA][DirectoryList::PATH];
        try {
            $destDir = $uploader->getDestDir();
            $uploadDir = $mediaRelativePath . DIRECTORY_SEPARATOR . $config->getBaseTmpMediaPath();
            $uploadDir = $this->_mediaDirectory->getAbsolutePath($uploadDir);

            if (!$uploader->isDirectoryWritable($uploadDir)) {
                $uploader->createDirectory($uploadDir);
            }
            if (!$uploader->setDestDir($uploadDir)) {
                $this->addRowError(
                    __('File directory \'%1\' is not writable.', $config->getBaseTmpMediaPath()),
                    null,
                    null,
                    null,
                    ProcessingError::ERROR_LEVEL_NOT_CRITICAL
                );
                throw new LocalizedException(
                    __('File directory \'%1\' is not writable.', $config->getBaseTmpMediaPath())
                );
            } else {
                $result = $uploader->move($swatchVisualFile);
                $newFile = $this->swatchHelperMedia->moveImageFromTmp($result['file']);
                $this->swatchHelperMedia->generateSwatchVariations($newFile);
                $uploader->setDestDir($destDir);
            }
        } catch (Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->output, 'error');
        }

        return $newFile;
    }

    /**
     * @param AdapterInterface $connection
     * @param ResourceModel $resource
     * @param int $optionId
     * @param array $swatchData
     * @param int $attributeId
     * @return $this
     * @throws LocalizedException
     */
    protected function insertNewSwatchOption($connection, $resource, $optionId, $swatchData, $attributeId)
    {
        if (!isset($swatchData['type']) && !isset($swatchData['value'])) {
            $table = $resource->getTable('eav_attribute_option_swatch');
            $data = [
                'option_id' => $optionId,
                'store_id' => 0,
                'type' => Swatch::SWATCH_TYPE_TEXTUAL,
                'value' => $swatchData,
            ];
            $connection->insert($table, $data);
            $this->cachedSwatchOptions[$attributeId][$optionId] = $data;
        } else {
            if ($swatchData['type'] == Swatch::SWATCH_TYPE_VISUAL_IMAGE) {
                $swatchData['value'] = $this->uploadVisualSwatchFile($swatchData['value']);
            }
            if ($swatchData['value']) {
                $table = $resource->getTable('eav_attribute_option_swatch');
                $data = [
                    'option_id' => $optionId,
                    'store_id' => 0,
                    'type' => $swatchData['type'],
                    'value' => $swatchData['value'],
                ];
                $connection->insert($table, $data);
                $this->cachedSwatchOptions[$attributeId][$optionId] = $data;
            }
        }

        return $this;
    }

    /**
     * @param $urlKey
     * @param $sku
     * @param $storeId
     *
     * @return string
     */
    protected function isDuplicateUrlKey($urlKey, $sku, $storeId)
    {
        $result = false;
        $urlKeyHtml = $urlKey . $this->getProductUrlSuffix();
        $resource = $this->getResource();
        $select = $this->_connection->select()->from(
            ['url_rewrite' => $resource->getTable('url_rewrite')],
            ['request_path', 'store_id']
        )->joinLeft(
            ['cpe' => $resource->getTable('catalog_product_entity')],
            'cpe.entity_id = url_rewrite.entity_id'
        )->where("request_path='$urlKey' OR request_path='$urlKeyHtml'")
            ->where('store_id IN (?)', $storeId)
            ->where('cpe.sku not in (?)', $sku);
        $isDuplicate = $this->_connection->fetchAssoc(
            $select
        );
        if (!empty($isDuplicate)) {
            $result = true;
        }
        return $result;
    }

    /**
     * @param array $rowData
     * @return string
     * @throws Exception
     */
    protected function getUrlKey($rowData)
    {
        $url = $this->productUrl->formatUrlKey(parent::getUrlKey($rowData));
        $this->urlKeyManager->addUrlKeys($rowData[self::COL_SKU], $url);
        return $url;
    }

    /**
     * @param array $rowData
     * @return string
     * @throws Exception
     */
    protected function getProductUrlKey($rowData)
    {
        if (isset($this->_parameters['enable_product_url_pattern']) &&
            $this->_parameters['enable_product_url_pattern'] === '1' &&
            isset($this->_parameters['product_url_pattern']) &&
            !empty($this->_parameters['product_url_pattern']) &&
            $this->validateProductUrlPattern($this->_parameters['product_url_pattern'])
        ) {
            return $this->generateUrlKeyByPattern($rowData);
        } else {
            return $this->getUrlKey($rowData);
        }
    }

    /**
     * @return array
     */
    protected function getProductUrlPatternVariables()
    {
        return ['[product_name]', '[product_sku]'];
    }

    /**
     * @param string $productUrlPattern
     * @return bool
     */
    protected function validateProductUrlPattern($productUrlPattern)
    {
        foreach ($this->getProductUrlPatternVariables() as $variable) {
            if (strpos($productUrlPattern, $variable) !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array $rowData
     * @param $storeIds
     * @return string
     * @throws Exception
     */
    protected function generateUrlKeyByPattern($rowData, $storeIds = null)
    {
        $productEntityLinkField = $this->getProductEntityLinkField();
        $sku = $this->getCorrectSkuAsPerLength($rowData);
        $name = $rowData[self::COL_NAME] ?? '';
        if ($this->isSkuExist($sku)) {
            $exiting = $this->getExistingSku($sku);
            if (!$name) {
                $attr = $this->retrieveAttributeByCode(self::COL_NAME);
                $select = $this->getConnection()->select()
                    ->from($attr->getBackendTable(), ['value'])
                    ->where($productEntityLinkField . ' = (?)', $exiting['entity_id'])
                    ->where('attribute_id = (?)', $attr->getAttributeId());
                $name = $this->getConnection()->fetchOne($select);
            }
        }
        if ($storeIds === null) {
            $storeIds = $this->getStoreIds();
        }
        $urlKey = str_replace(
            $this->getProductUrlPatternVariables(),
            [$name, $sku],
            $this->_parameters['product_url_pattern']
        );
        $urlKey = $this->productUrl->formatUrlKey($urlKey);
        $isDuplicate = $this->isDuplicateUrlKey($urlKey, $sku, $storeIds);
        if ($isDuplicate && $this->urlKeyManager->isUrlKeyExist($sku, $urlKey)) {
            $urlKey = $this->productUrl->formatUrlKey(
                $name . '-' . $sku
            );
        }
        $rowData[self::URL_KEY] = $urlKey;
        $this->urlKeyManager->addUrlKeys($sku, $urlKey);
        return $urlKey;
    }

    /**
     * Divide additionalImages for old Magento version
     * @param $rowData
     *
     * @return mixed
     */
    protected function checkAdditionalImages($rowData)
    {
        return $this->importImageProcessor->checkAdditionalImages($rowData);
    }

    /**
     * @param array $rowData
     * @return array
     */
    public function stripSlashes(array $rowData)
    {
        foreach ($rowData as $key => $val) {
            if ($key === '') {
                continue;
            }
            if (!empty($val)) {
                $rowData[$key] = stripslashes((string) $val);
            }
        }

        return $rowData;
    }

    /**
     * @param array $rowData
     *
     * @return array
     */
    public function prepareRowForDb(array $rowData)
    {
        $rowData = $this->customFieldsMapping($rowData);

        $this->stripSlashes($rowData);

        static $lastSku = null;

        if (Import::BEHAVIOR_DELETE == $this->getBehavior()) {
            return $rowData;
        }

        $lastSku = $this->getCorrectSkuAsPerLength($rowData);

        if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
            $checkSku = strtolower($lastSku);
        } else {
            $checkSku = $lastSku;
        }
        if (isset($this->_oldSku[$checkSku]) && $this->_oldSku[$checkSku]) {
            $newSku = $this->skuProcessor->getNewSku($lastSku);
            if (isset($rowData[self::COL_ATTR_SET]) && !$rowData[self::COL_ATTR_SET]) {
                $rowData[self::COL_ATTR_SET] = $newSku['attr_set_code'];
            }
            if (isset($rowData[self::COL_TYPE]) && !$rowData[self::COL_TYPE]) {
                $rowData[self::COL_TYPE] = $newSku['type_id'];
            }
        }

        return $rowData;
    }

    /**
     * @param $rowData
     *
     * @return mixed
     */
    public function applyCategoryLevelSeparator($rowData)
    {
        $defaultCategoryName = '';
        $importCategoryName = '';
        if (isset($this->_parameters['root_category_id']) && $this->_parameters['root_category_id'] > 0) {
            $importCategoryId = (int)$this->_parameters['root_category_id'];
            /** @var \Magento\Catalog\Model\Category $importCategory */
            $importCategory = $this->categoryProcessor->getCategoryById($importCategoryId);
            if ($importCategory) {
                if ($importCategory->getParentCategory()->getId() != 1) {
                    $importCategoryName = $defaultCategoryName = $importCategory->getParentCategory()->getName();
                }
                if ((int)$importCategory->getId() === $importCategoryId && $importCategoryName !== '') {
                    $importCategoryName .= '/' . $importCategory->getName();
                } else {
                    $defaultCategoryName = $importCategoryName = $importCategory->getName();
                }
            }
        } elseif (isset($rowData['_root_category'])) {
            $importCategoryName = $defaultCategoryName = $rowData['_root_category'];
        }
        if (isset($rowData[self::COL_CATEGORY]) && $rowData[self::COL_CATEGORY]) {
            if ($this->_parameters['category_levels_separator'] !== '/') {
                $rowData[self::COL_CATEGORY] = str_replace("/", "\/", $rowData[self::COL_CATEGORY]);
            }
            $rowData[self::COL_CATEGORY] = str_replace(
                $this->_parameters['category_levels_separator'],
                '/',
                $rowData[self::COL_CATEGORY]
            );

            $rowCategories = explode('/', $rowData[self::COL_CATEGORY]);
            $finalRowCat = [];
            foreach ($rowCategories as $rowCat) {
                if ($rowCat == '') {
                    continue;
                }
                $finalRowCat[] = $rowCat;
            }
            $rowData[self::COL_CATEGORY] = implode('/', $finalRowCat);
        }
        $categories = [];
        if ($defaultCategoryName && $importCategoryName && isset($rowData[self::COL_CATEGORY])) {
            foreach (explode($this->_parameters['categories_separator'], $rowData[self::COL_CATEGORY]) as $category) {
                if (strpos(trim($category), $defaultCategoryName) !== false) {
                    $categories[] = trim($category);
                } else {
                    $categories[] = $importCategoryName . '/' . trim($category);
                }
            }
            $rowData[self::COL_CATEGORY] = implode($this->_parameters['categories_separator'], $categories);
        } elseif ($importCategoryName) {
            $rowData[self::COL_CATEGORY] = $importCategoryName;
        }
        return $rowData;
    }

    /**
     * @param $array
     *
     * @return array
     */
    protected function deleteEmpty($array)
    {
        if (isset($array[self::COL_SKU])) {
            unset($array[self::COL_SKU]);
        }
        $newElement = [];
        foreach ($array as $key => $element) {
            if (strlen($element)) {
                $newElement[$key] = $element;
            }
        }

        return $newElement;
    }

    protected function getCategories($rowData)
    {
        if (isset($rowData[self::COL_STORE])) {
            $this->categoryProcessor->setStoreId($this->storeResolver->getStoreCodeToId($rowData[self::COL_STORE]));
        }
        $this->categoryProcessor->setGeneratUrl($this->_parameters['generate_url']);
        $this->categoryProcessor->setResource($this->getResource());
        $ids = $this->categoryProcessor->getRowCategories($rowData, $this->_parameters['categories_separator']);
        foreach ($this->categoryProcessor->getFailedCategories() as $error) {
            $this->errorAggregator->addError(
                AbstractEntity::ERROR_CODE_CATEGORY_NOT_VALID,
                ProcessingError::ERROR_LEVEL_NOT_CRITICAL,
                $rowData['rowNum'],
                self::COL_CATEGORY,
                __('Category "%1" has not been created.', $error['category'])
                . ' ' . $error['exception']->getMessage()
            );
        }

        return $ids;
    }

    /**
     * @param $rowData
     * @return mixed
     * @throws NoSuchEntityException
     */
    protected function applyPriceRules($rowData)
    {
        if (!empty($this->_parameters['price_rules'])) {
            $priceRules = $this->_parameters['price_rules'];

            foreach ($priceRules as $priceRule) {
                $applyRule = true;

                if (isset($priceRule['price_rules_conditions_hidden']['rule']['conditions'])) {
                    $conditions = $priceRule['price_rules_conditions_hidden']['rule']['conditions'];
                    $aggr = $conditions[1]['aggregator'];
                    $aggrValue = $conditions[1]['value'];

                    $data = [
                        'conditions' => $priceRule['price_rules_conditions_hidden']['rule']['conditions'],
                        'row' => $rowData,
                        'aggregator' => $aggr,
                        'value' => $aggrValue,
                        'categories' => isset($this->categoriesCache[$this->getCorrectSkuAsPerLength($rowData)]) ?
                            array_keys($this->categoriesCache[$this->getCorrectSkuAsPerLength($rowData)]) : [],
                    ];
                    if (empty($data['categories'])) {
                        $checkSku = $this->getCorrectSkuAsPerLength($rowData);
                        if (isset($this->_oldSku[strtolower($checkSku)])) {
                            /** @var \Magento\Catalog\Model\Product $product */
                            $product = $this->productRepository->get($checkSku);
                            $data['categories'] = $product->getCategoryIds();
                        }
                    }
                    $applyRule = $this->priceRuleConditionFactory->create()->validatePriceRuleConditions($data);
                }

                if ($applyRule) {
                    if ($priceRule['apply'] == 'fixed') {
                        $rowData['price'] += $priceRule['value'];
                    } else {
                        $rowData['price'] *= 1 + $priceRule['value'] / 100;
                    }
                }
            }
        }

        if (isset($this->_parameters['round_up_prices'], $rowData['price'])
            && $this->_parameters['round_up_prices'] > 0) {
            $rowData['price'] = $this->roundPrice($rowData['price']);
        }

        if (isset($this->_parameters['round_up_special_price'], $rowData['special_price'])
            && $this->_parameters['round_up_special_price'] > 0) {
            $rowData['special_price'] = $this->roundPrice($rowData['special_price']);
        }

        return $rowData;
    }

    /**
     * Round price to *.49 or to *.99
     *
     * @param $num
     *
     * @return float
     */
    protected function roundPrice($num): float
    {
        $fln = $num - floor($num);
        if ($fln > 0 && $fln < 0.5) {
            $fln = 0.49;
        } else {
            $fln = 0.99;
        }

        return floor($num) + $fln;
    }

    /**
     * @param array $data
     * @param string $rowSku
     *
     * @return array
     */
    protected function getTierPrices($data, $rowSku)
    {
        $tierPrices = [];

        if (!empty($data['tier_prices'])) {
            $tiers = explode("|", $data['tier_prices']);
            $groups = $this->groupFactory->create()->getCollection()->toOptionArray();
            $newGroups = [];
            foreach ($groups as $group) {
                $newGroups[$group['label']] = $group['value'];
            }
            $websites = $this->websiteFactory->create()->getCollection()->toOptionArray();
            $newWebsites = [0 => self::VALUE_ALL];
            foreach ($websites as $website) {
                $newWebsites[$website['label']] = $website['value'];
            }
            foreach ($tiers as $field) {
                $elements = array_map('trim', explode($this->getMultipleValueSeparator(), $field));
                $isAllGroup = 0;
                if ($elements[0] == __('ALL GROUPS')) {
                    $isAllGroup = 1;
                }

                if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
                    $tierPrices[$rowSku][] = [
                        'all_groups' => $isAllGroup,
                        'customer_group_id' => (isset($elements[0]) && isset($newGroups[$elements[0]])) ?
                            $newGroups[$elements[0]] : 0,
                        'qty' => (isset($elements[1])) ? $elements[1] : 0,
                        'value' => (isset($elements[2])) ? $elements[2] : 0,
                        'percentage_value' => (isset($elements[3])) ?
                            (!empty($elements[3]) ? $elements[3] : null) : null,
                        'website_id' => (isset($elements[4]) && isset($newWebsites[$elements[4]])) ?
                            $newWebsites[$elements[4]] : 0,
                    ];
                } else {
                    $tierPrices[$rowSku][] = [
                        'all_groups' => $isAllGroup,
                        'customer_group_id' => (isset($elements[0]) && isset($newGroups[$elements[0]])) ?
                            $newGroups[$elements[0]] : 0,
                        'qty' => (isset($elements[1])) ? $elements[1] : 0,
                        'value' => (isset($elements[2])) ? $elements[2] : 0,
                        'website_id' => (isset($elements[3]) && isset($newWebsites[$elements[3]])) ?
                            $newWebsites[$elements[3]] : 0,
                    ];
                }
            }
        }
        return $tierPrices;
    }

    /**
     * Save product tier prices.
     *
     * @param array $tierPriceData
     * @return $this
     */
    protected function _saveProductTierPrices(array $tierPriceData)
    {
        static $tableName = null;

        if (!$tableName) {
            $tableName = $this->_resourceFactory->create()->getTable('catalog_product_entity_tier_price');
        }
        if ($tierPriceData) {
            $tierPriceIn = [];
            $delProductId = [];

            foreach ($tierPriceData as $delSku => $tierPriceRows) {
                $productId = $this->skuProcessor->getNewSku($delSku)[$this->getProductEntityLinkField()];
                $delProductId[] = $productId;

                foreach ($tierPriceRows as $row) {
                    $row[$this->getProductEntityLinkField()] = $productId;
                    $tierPriceIn[] = $row;
                }
            }
            if (Import::BEHAVIOR_APPEND != $this->getBehavior()) {
                $this->_connection->delete(
                    $tableName,
                    $this->_connection->quoteInto("{$this->getProductEntityLinkField()} IN (?)", $delProductId)
                );
            }
            if ($tierPriceIn) {
                $this->_connection->insertOnDuplicate($tableName, $tierPriceIn, ['value', 'percentage_value']);
            }
        }
        return $this;
    }

    /**
     * @param $data
     * @param array $existingImages
     * @return $this
     */
    protected function saveConfigurationVariations($data, $existingImages = [])
    {
        if (!empty($data)) {
            $simpleValAttr = [];
            if (isset($this->_parameters['copy_simple_value'])) {
                foreach ($this->_parameters['copy_simple_value'] as $simpleValConfig) {
                    $simpleValAttr[] = $simpleValConfig['copy_simple_value_attributes'];
                }
            }
            /**
             * @var string $skuConf
             * @var array $elements
             */
            foreach ($data as $skuConf => $elements) {
                $skuConf = (string) $skuConf;
                if (count($elements) < 1) {
                    continue;
                }
                if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
                    $checkSku = mb_strtolower($skuConf);
                }
                $categories = $websites = [];
                $additionalRows = [];
                $changeAttributes = [];
                $mediaGallery = [];
                $storeIds = $this->getStoreIds();
                $product = null;
                try {
                    $collection = $this->collectionFactory->create()
                        ->addFieldToFilter('sku', $skuConf)
                        ->addFieldToFilter('type_id', 'configurable')
                        ->addAttributeToSelect('*');
                    $this->addLogWriteln(__('Configure variations for SKU:%1', $skuConf), $this->output, 'info');
                    if ($this->_parameters['configurable_create'] && !$collection->getSize()) {
                        try {
                            /** @var Collection $collectionChild */
                            $collectionChild = $this->collectionFactory->create();
                            $collectionChild->addFieldToFilter('sku', $elements[0][self::COL_SKU])
                                ->addAttributeToSelect('*');
                            /** @var \Magento\Catalog\Model\Product $child */
                            $child = $collectionChild->getFirstItem();
                            $data = [];
                            $data[self::COL_SKU] = $skuConf;
                            $data[self::COL_NAME] = $skuConf;
                            foreach ($simpleValAttr as $attr) {
                                $data[$attr] = $child->getData($attr);
                            }
                            $data['attribute_set_id'] = $child->getAttributeSetId();
                            $data['type_id'] = 'configurable';
                            $data['website_ids'] = $child->getWebsiteIds();
                            $websites[$skuConf] = $child->getWebsiteIds();
                            $data['category_ids'] = $child->getCategoryIds();
                            $data['visibility'] = Visibility::VISIBILITY_BOTH;
                            $data['has_options'] = $child->getData('has_options');
                            $product = $this->_proxyProdFactory->create();
                            if ($this->_parameters['generate_url'] && isset($data[self::COL_NAME])) {
                                $storeIds = $this->getStoreIds();
                                $data = $this->generateUrlKey($data, $storeIds);
                            }
                            $product->setData($data);
                            $product->setQuantityAndStockStatus(['qty' => 0, 'is_in_stock' => 1]);
                            $product = $this->productRepository->save($product);
                            $entityLinkField = $this->getProductEntityLinkField();
                            $data[$entityLinkField] = $product->getId();
                            $this->skuProcessor->addNewSku($skuConf, $data);
                            $this->_oldSku[strtolower($skuConf)] = [
                                'type_id' => "configurable",
                                'attr_set_id' => $child->getAttributeSetId(),
                                $entityLinkField => $product->getId(),
                                'supported_type' => true,
                            ];
                            $productId = $product->getId();
                            foreach ($simpleValAttr as $attr) {
                                switch ($attr) {
                                    case SystemOptions::RELATED_PRODUCT_ATTRIBUTE:
                                        $this->productLinkData[$productId]['type'][] = ProductLink::LINK_TYPE_RELATED;
                                        break;
                                    case SystemOptions::UP_SELLS_PRODUCT_ATTRIBUTE:
                                        $this->productLinkData[$productId]['type'][] = ProductLink::LINK_TYPE_UPSELL;
                                        break;
                                    case SystemOptions::CROSS_SELLS_PRODUCT_ATTRIBUTE:
                                        $this->productLinkData[$productId]['type'][] = ProductLink::LINK_TYPE_CROSSSELL;
                                        break;
                                }
                            }
                        } catch (LocalizedException $e) {
                            $this->addLogWriteln($e->getMessage(), $this->output, 'error');
                        }
                    } else {
                        if ($collection->getSize()) {
                            /** @var \Magento\Catalog\Model\Product $product */
                            $product = $collection->getFirstItem();

                            if ($product->getSku() == $skuConf) {
                                /** @var Collection $collectionChild */
                                $collectionChild = $this->collectionFactory->create();
                                $collectionChild->addFieldToFilter('sku', $elements[0][self::COL_SKU])
                                    ->addAttributeToSelect('*');
                                /** @var \Magento\Catalog\Model\Product $child */
                                $child = $collectionChild->getFirstItem();
                                $_updateData = [];
                                $productId = $product->getId();
                                foreach ($simpleValAttr as $attr) {
                                    switch ($attr) {
                                        case SystemOptions::RELATED_PRODUCT_ATTRIBUTE:
                                            $this->productLinkData[$productId]['type'][] =
                                                ProductLink::LINK_TYPE_RELATED;
                                            break;
                                        case SystemOptions::UP_SELLS_PRODUCT_ATTRIBUTE:
                                            $this->productLinkData[$productId]['type'][] =
                                                ProductLink::LINK_TYPE_UPSELL;
                                            break;
                                        case SystemOptions::CROSS_SELLS_PRODUCT_ATTRIBUTE:
                                            $this->productLinkData[$productId]['type'][] =
                                                ProductLink::LINK_TYPE_CROSSSELL;
                                            break;
                                        default:
                                            $_updateData[$attr] = $child->getData($attr);
                                            break;
                                    }
                                }
                                $objectManager = ObjectManager::getInstance();
                                $productAction = $objectManager->create(Action::class);
                                $productAction->updateAttributes([$product->getId()], $_updateData, 0);
                                $websites[$skuConf] = $child->getWebsiteIds();
                                $childCategory = $child->getCategoryIds();
                                $configCat = $product->getCategoryIds();
                                $configCategory = [];
                                $del = true;
                                $deleteCat = array_diff($configCat, $childCategory);
                                if (!isset($this->_parameters['remove_product_categories'])
                                    && $this->_parameters['remove_product_categories'] > 0
                                ) {
                                    $del = false;
                                }
                                if (!array_key_exists($skuConf, $this->categoriesCache)) {
                                    $this->categoriesCache[$skuConf] = [];
                                    $categories[$skuConf] = [];
                                    $configCategory = [];
                                }
                                foreach (array_merge($childCategory, $configCat) as $categoryId) {
                                    if ($del === true && in_array($categoryId, $deleteCat, false)) {
                                        $configCategory[$categoryId] = false;
                                    } else {
                                        $configCategory[$categoryId] = true;
                                    }
                                }
                                $categories[$skuConf] = $configCategory;
                            }
                            if ($product->getTypeId() != 'configurable') {
                                $product->setTypeId('configurable');
                                $product = $this->productRepository->save($product);
                            }
                        } else {
                            $this->addLogWriteln(
                                __(
                                    'Configurable Product for sku "%1" not created before. Turn on feature to create ' .
                                    'configurable product on the fly',
                                    $skuConf
                                ),
                                $this->getOutput(),
                                'error'
                            );

                            continue;
                        }
                    }

                    if (isset($this->_parameters['remove_images'])
                        && array_key_exists($skuConf, $existingImages)
                        && $this->_parameters['remove_images'] == 1
                    ) {
                        $this->removeExistingImages($existingImages[$skuConf]);
                        unset($existingImages[$skuConf]);
                    }

                    foreach ($this->_imagesArrayKeys as $fieldImage) {
                        if ($fieldImage === '_media_image' || $fieldImage === ProductVideo::VIDEO_URL_COLUMN) {
                            continue;
                        }
                        $data[$fieldImage] = $child->getData($fieldImage);
                        $attributeChange = $this->retrieveAttributeByCode($fieldImage);
                        $attrId = $attributeChange->getId();
                        $attrTable = $attributeChange->getBackend()->getTable();
                        $attrValue = $child->getData($fieldImage);
                        if (!isset($changeAttributes[$attrTable][$checkSku][$attrId][0]) && !empty($attrValue)) {
                            $changeAttributes[$attrTable][$skuConf][$attrId][0] = $attrValue;
                            if (version_compare($this->productMetadata->getVersion(), '2.2.4', '>=') ||
                                strpos($this->getProductMetadata()->getVersion(), '1.0.0') !== false) {
                                $mediaGallery[Store::DEFAULT_STORE_ID][$skuConf][] = [
                                    'attribute_id' => $this->getMediaGalleryAttributeId(),
                                    'label' => '',
                                    'position' => 1,
                                    'disabled' => '0',
                                    'value' => $attrValue,
                                ];
                            } else {
                                $mediaGallery[$skuConf][] = [
                                    'attribute_id' => $this->getMediaGalleryAttributeId(),
                                    'label' => '',
                                    'position' => 1,
                                    'disabled' => '0',
                                    'value' => $attrValue,
                                ];
                            }
                        }
                    }

                    $vars = [];
                    $attributes = [];
                    $attributeChange = $this->retrieveAttributeByCode('visibility');
                    $attrTable = $attributeChange->getBackend()->getTable();

                    $attrValue = 1;

                    $attrId = $attributeChange->getId();
                    foreach ($elements as $element) {
                        $position = 0;
                        foreach ($element as $ki => $field) {
                            if ($ki != 'sku' && !empty($field)) {
                                if (!in_array($ki, $attributes)) {
                                    $attributes[] = $ki;
                                }
                                $vars['fields'][] = [
                                    'code' => $ki,
                                    'value' => $field,
                                ];
                            } else {
                                $vars[$ki] = $field;
                                if ($ki == 'sku') {
                                    foreach ($storeIds as $storeId) {
                                        if (!isset($changeAttributes[$attrTable][$field][$attrId][$storeId])) {
                                            $changeAttributes[$attrTable][$field][$attrId][$storeId] = $attrValue;
                                        }
                                    }
                                }
                            }
                        }
                        $vars['position'] = $position;
                        $position++;
                        $additionalRows[] = $vars;
                    }
                    $attributeValues = [];
                    $ids = [];
                    $configurableAttributesData = [];
                    $position = 0;
                    foreach ($attributes as $attribute) {
                        foreach ($additionalRows as $list) {
                            $attributeCollection = $this->attributeFactory->create()->getCollection();
                            $attributeCollection->addFieldToFilter('attribute_code', $attribute);
                            $value = [];
                            if (isset($list['fields'])) {
                                foreach ($list['fields'] as $item) {
                                    if ($item['code'] == $attribute) {
                                        $value = $item['value'];
                                        $collection = $this->collectionFactory->create();
                                        $collection->addFieldToFilter('sku', $list['sku']);
                                        if (!in_array($collection->getFirstItem()->getId(), $ids)) {
                                            $ids[] = $collection->getFirstItem()->getId();
                                        }
                                    }
                                }
                            }
                            if ($attributeCollection->getSize()) {
                                $attributeValues[$attribute][] = [
                                    'label' => $attribute,
                                    'attribute_id' => $attributeCollection->getFirstItem()->getId(),
                                    'value_index' => $value,
                                ];
                            }
                        }
                        if ($attributeCollection->getSize()) {
                            $attr = $attributeCollection->getFirstItem();
                            $configurableAttributesData[] =
                                [
                                    'attribute_id' => $attr->getId(),
                                    'code' => $attr->getAttributeCode(),
                                    'label' => $attr->getStoreLabel(),
                                    'position' => $position++,
                                    'values' => $attributeValues[$attribute],
                                ];
                        }
                    }

                    /**
                     * Check if attributes was added to target attribute set.
                     */
                    if (isset($product) && $product->getAttributeSetId() > 0) {
                        $invalidAttributes = [];
                        $attributeSetId = $product->getAttributeSetId();
                        foreach ($configurableAttributesData as $attribute) {
                            $attributeId = $attribute['attribute_id'];
                            $select = $this->_connection->select()
                                ->from(
                                    $this->getResource()->getTable('eav_entity_attribute'),
                                    'attribute_id'
                                )->where(
                                    'attribute_set_id = ?',
                                    $attributeSetId
                                )->where(
                                    'attribute_id = ?',
                                    $attributeId
                                );
                            $result = $this->_connection->fetchCol($select);
                            if (empty($result)) {
                                $invalidAttributes[] = $attribute['code'];
                            }
                        }
                        if (!empty($invalidAttributes)) {
                            throw new LocalizedException(
                                __(
                                    "Attributes '%1' is not attached to related attribute set.",
                                    implode(', ', $invalidAttributes)
                                )
                            );
                        }
                    }
                    if (!empty($mediaGallery)) {
                        $this->_saveMediaGallery($mediaGallery);
                    }
                    if (!empty($websites)) {
                        $this->_saveProductWebsites($websites, $product->getId(), true);
                    }
                    if (!empty($categories)) {
                        $this->_saveProductCategories($categories, $product->getId(), true);
                    }
                    if (!empty($changeAttributes)) {
                        $this->_saveProductAttributes($changeAttributes);
                    }
                    if ($product !== null) {
                        $this->saveCollectData($product, $configurableAttributesData, $ids);
                    }
                } catch (Exception $e) {
                    $this->getErrorAggregator()->addError(
                        $e->getCode(),
                        ProcessingError::ERROR_LEVEL_NOT_CRITICAL,
                        null,
                        null,
                        $e->getMessage()
                    );
                }
            }
        }

        return $this;
    }

    /**
     * Add related, cross-sell or up-sell products
     *
     * @param $configurableProductId
     * @param $typesLink
     * @throws LocalizedException
     */
    protected function addProductLinks($configurableProductId, $typesLink)
    {
        $resource = $this->_linkFactory->create();
        $mainTable = $this->productLink->getLinkCollection()->getMainTable();
        $nextLinkId = $this->_resourceHelper->getNextAutoincrement($mainTable);
        $allLinkedProducts = [];
        $positionRows = [];
        $select = $this->_connection->select()
            ->from($this->getResource()->getTable('catalog_product_relation'))
            ->where('parent_id=?', $configurableProductId);
        $simpleProductId = $this->_connection->fetchRow($select)['child_id'];
        $select = $this->_connection->select()
            ->from($mainTable)
            ->reset(Zend_Db_Select::COLUMNS)
            ->columns(['linked_product_id', 'link_type_id'])
            ->where('product_id=?', $simpleProductId);
        $linkedProducts = $this->_connection->fetchAll($select);
        $this->_connection->delete(
            $mainTable,
            "product_id=$configurableProductId"
        );
        $i = 0;
        foreach ($linkedProducts as $product) {
            if (in_array($product['link_type_id'], $typesLink['type'])) {
                $product['product_id'] = $configurableProductId;
                $product['link_id'] = $nextLinkId;
                $allLinkedProducts[] = $product;
                $positionRows[] = [
                    'link_id' => $nextLinkId,
                    'product_link_attribute_id' => $product['link_type_id'],
                    'value' => $i++,
                ];
                $nextLinkId++;
            }
        }
        if (isset($allLinkedProducts) && !empty($allLinkedProducts)) {
            $this->_connection->insertMultiple(
                $mainTable,
                $allLinkedProducts
            );
            $this->_connection->insertOnDuplicate(
                $resource->getAttributeTypeTable('int'),
                $positionRows,
                ['value']
            );
        }
    }

    /**
     * @param array $websiteData
     * @param null $productId
     * @param bool $config
     *
     * @return $this|MagentoProduct
     */
    protected function _saveProductWebsites(array $websiteData, $productId = null, $config = false)
    {
        static $productWebsiteTable = null;
        $removeWebsite = $this->_parameters['remove_product_website'] ?? 0;
        if ($removeWebsite || $productId) {
            if (!$productWebsiteTable) {
                $productWebsiteTable = $this->getResource()->getProductWebsiteTable();
            }
            if ($websiteData) {
                $newWebsiteData = [];
                $deletedProductIds = [];

                foreach ($websiteData as $productSku => $productWebsites) {
                    if ($this->onlyUpdate
                        && $removeWebsite > 0
                        && empty($productWebsites)
                    ) {
                        $this->addLogWriteln(
                            __('Product %1 Website Cannot be Cleared', $productSku),
                            $this->output,
                            'info'
                        );
                        continue;
                    }
                    if (!$config) {
                        $productId = $this->skuProcessor->getNewSku($productSku)['entity_id'];
                    }
                    $deletedProductIds[] = $productId;
                    if ($config) {
                        foreach ($productWebsites as $websiteId) {
                            $newWebsiteData[] = ['product_id' => $productId, 'website_id' => $websiteId];
                        }
                    } else {
                        foreach (array_keys($productWebsites) as $websiteId) {
                            $newWebsiteData[] = ['product_id' => $productId, 'website_id' => $websiteId];
                        }
                    }
                }
                $this->_connection->delete(
                    $productWebsiteTable,
                    $this->_connection->quoteInto('product_id IN (?)', $deletedProductIds)
                );

                if ($newWebsiteData) {
                    $this->_connection->insertOnDuplicate($productWebsiteTable, $newWebsiteData);
                }
            }
            return $this;
        }
        return parent::_saveProductWebsites($websiteData);
    }

    /**
     * @param \Magento\Catalog\Model\Product $product
     * @param array $configurableAttributesData
     * @param array $ids
     * @throws Exception
     */
    public function saveCollectData($product, $configurableAttributesData, $ids)
    {
        $productId = $product->getData($this->getProductEntityLinkField());

        $connection = $this->_connection;
        $resource = $this->getResource();
        $table = $resource->getTable('catalog_product_super_attribute');
        $labelTable = $resource->getTable('catalog_product_super_attribute_label');
        $linkTable = $resource->getTable('catalog_product_super_link');
        $relationTable = $resource->getTable('catalog_product_relation');
        $select = $connection->select()->from(
            ['m' => $table],
            ['product_id', 'attribute_id', 'product_super_attribute_id']
        )->where(
            'm.product_id IN ( ? )',
            [$productId]
        );

        //check if import source has different product variation for this product then delete current variations
        $counts = count($connection->fetchAll($select));
        if ($counts) {
            $isChangeOfAttributeVariation = false;
            if ($counts != count($configurableAttributesData)) {
                $isChangeOfAttributeVariation = true;
            } else {
                $currentattrConfig = $connection->fetchAll($select);
                $currentAttributeIds = [];
                if (!empty($currentattrConfig)) {
                    foreach ($currentattrConfig as $key => $value) {
                        $currentAttributeIds[] = $value['attribute_id'];
                    }
                }

                $newAttrIds = [];
                foreach ($configurableAttributesData as $elem) {
                    $newAttrIds[] = $elem['attribute_id'];
                }
                sort($currentAttributeIds);
                sort($newAttrIds);
                if ($currentAttributeIds != $newAttrIds) {
                    $isChangeOfAttributeVariation = true;
                }
            }
            if ($isChangeOfAttributeVariation) {
                $whereConditions = [
                    $connection->quoteInto('product_id = ?', $productId),
                ];
                $deleteRows = $connection->delete($table, $whereConditions);
            }
        }

        //inser new product variation for configurable product
        $counts = count($connection->fetchAll($select));
        if (!$counts) {
            foreach ($configurableAttributesData as $elem) {
                $data = [
                    'product_id' => $productId,
                    'attribute_id' => $elem['attribute_id'],
                    'position' => $elem['position'],
                ];
                $connection->insertOnDuplicate($table, $data);
            }

            foreach ($connection->fetchAll($select) as $row) {
                $attrId = $row['attribute_id'];
                $superId = $row['product_super_attribute_id'];
                foreach ($configurableAttributesData as $elem) {
                    if ($elem['attribute_id'] == $attrId) {
                        $data = ['product_super_attribute_id' => $superId, 'value' => $elem['label']];
                        $connection->insertOnDuplicate($labelTable, $data);
                    }
                }
            }
        }
        $first = 0;
        foreach ($ids as $id) {
            $data = ['product_id' => $id, 'parent_id' => $productId];
            $connection->insertOnDuplicate($linkTable, $data);
            if ($this->manager->isEnabled('Firebear_ConfigurableProducts') && !$first) {
                $connection->insertOnDuplicate(
                    $resource->getTable('icp_catalog_product_default_super_link'),
                    $data
                );
                $first = 1;
            }
            $relData = ['child_id' => $id, 'parent_id' => $productId];
            $connection->insertOnDuplicate($relationTable, $relData);
        }
    }

    /**
     * @param string $sku
     * @return bool
     */
    public function isExist($sku)
    {
        if ($this->onlyUpdate) {
            $collectionUpdate = $this->collectionFactory->create()->addFieldToFilter(
                self::COL_SKU,
                $sku
            );
            if (!$collectionUpdate->getSize()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Load categories map
     *
     * @param string $fieldName
     * @return array
     * @throws LocalizedException
     */
    public function getCategoriesMap($fieldName)
    {
        $bunchRows = [];
        $categories = [];
        $source = $this->_getSource();
        $source->rewind();
        $i = 1;
        while ($source->valid() || $bunchRows) {
            if ($source->valid()) {
                $rowData = $source->current();
                if (isset($rowData[$fieldName])) {
                    $categories[] = $rowData[$fieldName];
                }
                $i++;
                $source->next();
            }
        }

        return $categories;
    }

    /**
     * Validate data
     *
     * @param int $saveBunches
     * @return ProcessingErrorAggregatorInterface
     * @throws LocalizedException
     * @throws Zend_Validate_Exception
     * @throws Exception
     */
    public function validateData($saveBunches = 1)
    {
        if ($this->_parameters['behavior'] == Import::FIREBEAR_ONLY_UPDATE) {
            $this->onlyUpdate = 1;
            $this->_parameters['behavior'] = Import::BEHAVIOR_APPEND;
        } elseif ($this->_parameters['behavior'] == Import::FIREBEAR_ONLY_ADD) {
            $this->onlyAdd = true;
            $this->_parameters['behavior'] = Import::BEHAVIOR_APPEND;
        }

        if (isset($this->_parameters['output'])) {
            $this->output = $this->_parameters['output'];
        }

        $this->_initTypeModels();
        if (!$this->_dataValidated) {
            $this->getErrorAggregator()->clear();
            // do all permanent columns exist?
            $platformModel = null;
            $absentColumns =
                array_diff($this->_permanentAttributes, $this->getSource()->getColNames());
            $this->addErrors(self::ERROR_CODE_COLUMN_NOT_FOUND, $absentColumns);

            // check attribute columns names validity
            $columnNumber = 0;
            $emptyHeaderColumns = [];
            $invalidColumns = [];
            $invalidAttributes = [];
            foreach ($this->getSource()->getColNames() as $columnName) {
                $this->addLogWriteln(__('Checked column %1', $columnNumber), $this->output);
                $columnNumber++;
                if (!$this->isAttributeParticular($columnName)) {
                    /**
                     * Check syntax when attribute should be created on the fly
                     */
                    $createValuesAllowed = (bool)$this->scopeConfig->getValue(
                        Import::CREATE_ATTRIBUTES_CONF_PATH,
                        ScopeInterface::SCOPE_STORE
                    );
                    if ($createValuesAllowed && preg_match('/^(attribute\|).+/', $columnName)) {
                        $attrCodes = [];
                        $columnData = explode('|', $columnName);
                        $columnData = $this->prepareAttributeData($columnData);
                        $attribute = $this->attributeFactory->create();
                        $attribute->loadByCode(\Magento\Catalog\Model\Product::ENTITY, $columnData['attribute_code']);
                        if (!$attribute->getId()) {
                            $this->prepareAttributesData($columnData);
                            $attribute->setBackendType(
                                $attribute->getBackendTypeByInput($columnData['frontend_input'])
                            );
                            $defaultValueField = $attribute->getDefaultValueByInput($columnData['frontend_input']);
                            if (!$defaultValueField && isset($columnData['default_value'])) {
                                unset($columnData['default_value']);
                            }
                            $columnData['source_model'] = $this->productHelper->getAttributeSourceModelByInputType(
                                $columnData['frontend_input']
                            );
                            $columnData['backend_model'] = $this->productHelper->getAttributeBackendModelByInputType(
                                $columnData['frontend_input']
                            );

                            $attribute->addData($columnData);
                            try {
                                $attribute->save();
                            } catch (Exception $e) {
                                $invalidColumns[] = $columnName;
                            }
                            $attrSetCodes = explode(',', $columnData[self::ATTRIBUTE_SET_COLUMN]);
                            foreach ($attrSetCodes as $attrSetCode) {
                                if (isset($this->_attrSetNameToId[$attrSetCode])) {
                                    $attrSetId = $this->_attrSetNameToId[$attrSetCode];
                                    $attrGroupCode = isset($columnData[self::ATTRIBUTE_SET_GROUP])
                                        ? $columnData[self::ATTRIBUTE_SET_GROUP] : 'product-details';
                                    if (!isset($this->_attributeSetGroupCache[$attrSetId])) {
                                        $groupCollection =
                                            $this->groupCollectionFactory->create()->setAttributeSetFilter(
                                                $attrSetId
                                            )->load();
                                        foreach ($groupCollection as $group) {
                                            $attributeGroupCode = $group->getAttributeGroupCode();
                                            $this->_attributeSetGroupCache[$attrSetId][$attributeGroupCode] =
                                                $group->getAttributeGroupId();
                                        }
                                    }
                                    foreach ($this->_attributeSetGroupCache[$attrSetId] as $groupCode => $groupId) {
                                        if ($groupCode == $attrGroupCode) {
                                            $attribute->setAttributeSetId($attrSetId);
                                            $attribute->setAttributeGroupId($groupId);
                                            try {
                                                $attribute->save();
                                                $attrCodes[] = $attribute->getAttributeCode();
                                            } catch (Exception $e) {
                                                $this->addLogWriteln($e->getMessage(), $this->output, 'error');
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        $this->_specialAttributes = array_merge($this->_specialAttributes, $attrCodes);
                    }

                    if (trim($columnName) == '') {
                        $emptyHeaderColumns[] = $columnNumber;
                    } elseif ($this->needColumnCheck && !in_array($columnName, $this->getValidColumnNames())) {
                        $invalidAttributes[] = $columnName;
                    }
                }
            }

            $this->addErrors(self::ERROR_CODE_INVALID_ATTRIBUTE, $invalidAttributes);
            $this->addErrors(self::ERROR_CODE_COLUMN_EMPTY_HEADER, $emptyHeaderColumns);
            $this->addErrors(self::ERROR_CODE_COLUMN_NAME_INVALID, $invalidColumns);
            $this->addLogWriteln(__('Finish checking columns'), $this->output);
            $this->addLogWriteln(__('Errors count: %1', $this->getErrorAggregator()->getErrorsCount()), $this->output);
            if (!$this->getErrorAggregator()->getErrorsCount()) {
                if ($saveBunches) {
                    $this->addLogWriteln(__('Start saving bunches'), $this->output);
                    $this->mergeFieldsMap();

                    $platform = null;
                    $platformName = $this->_parameters['platforms'] ?? null;
                    if ($platformName && is_string($platformName) && $platformName != 'magento2') {
                        $platform = $this->helper->getPlatformModel($platformName);
                    }
                    /* use platform @todo remove this */
                    if ($platform && method_exists($platform, 'saveValidatedBunches')) {
                        $platform->saveValidatedBunches(
                            $this->_getSource(),
                            $this->_resourceHelper->getMaxDataSize(),
                            $this->_importExportData->getBunchSize(),
                            $this->_dataSourceModel,
                            $this->_parameters,
                            $this->getEntityTypeCode(),
                            $this->getBehavior(),
                            $this->_processedRowsCount,
                            $this->getMultipleValueSeparator(),
                            $this
                        );
                    } else {
                        $this->_saveValidatedBunches();
                    }

                    $this->addLogWriteln(__('Finish saving bunches'), $this->output);
                    $this->_dataValidated = true;
                }
            }
        }

        return $this->getErrorAggregator();
    }

    /**
     * @return $this
     * @throws LocalizedException
     */
    protected function _initTypeModels()
    {
        $this->_importConfig = $this->fireImportConfig;
        $productTypes = $this->_importConfig->getEntityTypes($this->getEntityTypeCode());
        foreach ($productTypes as $productTypeName => $productTypeConfig) {
            $class = $productTypeConfig['model'];
            $class::$commonAttributesCache = [];
            $class::$attributeCodeToId = [];
        }
        try {
            parent::_initTypeModels();
        } catch (\Exception $exception) {
            $this->addLogWriteln($exception->getMessage(), $this->getOutput(), 'error');
            throw new LocalizedException($exception->getMessage());
        }

        return $this;
    }

    /**
     * Checks and sets appropriate data for swatch attribute
     *
     * @param array $data
     */
    protected function prepareAttributesData(&$data)
    {
        if (isset($data['frontend_input'])) {
            switch ($data['frontend_input']) {
                case 'swatch_visual':
                    $data[Swatch::SWATCH_INPUT_TYPE_KEY] = Swatch::SWATCH_INPUT_TYPE_VISUAL;
                    $data['frontend_input'] = 'select';
                    break;
                case 'swatch_text':
                    $data[Swatch::SWATCH_INPUT_TYPE_KEY] = Swatch::SWATCH_INPUT_TYPE_TEXT;
                    $data['use_product_image_for_swatch'] = 0;
                    $data['frontend_input'] = 'select';
                    break;
                case 'select':
                    $data[Swatch::SWATCH_INPUT_TYPE_KEY] = Swatch::SWATCH_INPUT_TYPE_DROPDOWN;
                    $data['frontend_input'] = 'select';
                    break;
            }
        }
    }

    /**
     * mergeFieldsMap function
     */
    protected function mergeFieldsMap()
    {
        if (isset($this->_parameters['map'])) {
            $newAttributes = [];
            foreach ($this->_parameters['map'] as $field) {
                if (!$field['import'] || !empty($field['import'])) {
                    $field['import'] = $field['system'];
                }
                $attribute = $this->getResource()->getAttribute($field['system']);
                if ($attribute) {
                    $newAttributes[$attribute->getAttributeCode()] = $field['import'];
                } else {
                    $newAttributes[$field['system']] = $field['import'];
                }
            }

            $this->_fieldsMap = array_merge($this->_fieldsMap, $newAttributes);
        }
    }

    /**
     * @return $this|MagentoProduct
     * @throws LocalizedException
     * @throws Zend_Validate_Exception
     * @throws Exception
     */
    protected function _saveValidatedBunches()
    {
        $_currentRowSkus = [];
        $source = $this->_getSource();
        $currentDataSize = 0;
        $bunchRows = [];
        $startNewBunch = false;
        $nextRowBackup = [];
        $maxDataSize = $this->_resourceHelper->getMaxDataSize();
        $bunchSize = $this->_importExportData->getBunchSize();
        $skuSet = [];

        $source->rewind();
        $this->_dataSourceModel->cleanBunches();
        $file = null;
        $jobId = null;
        if (isset($this->_parameters['file'])) {
            $file = $this->_parameters['file'];
        }
        if (isset($this->_parameters['job_id'])) {
            $jobId = $this->_parameters['job_id'];
        }

        while ($source->valid() || $bunchRows) {
            if ($startNewBunch || !$source->valid()) {
                if (version_compare($this->productMetadata->getVersion(), '2.2.4', '>=')) {
                    $bunchRows = $this->prepareCustomOptionRows($bunchRows);
                }
                if (empty($this->_parameters['generate_url'])) {
                    $bunchRows = $this->findUrlKeyDuplicates($bunchRows);
                }
                $this->addLogWriteln(__('Saving Validated Bunches'), $this->output, 'info');

                $this->_dataSourceModel->saveBunches(
                    $this->getEntityTypeCode(),
                    $this->getBehavior(),
                    $jobId,
                    $file,
                    $bunchRows
                );
                $bunchRows = $nextRowBackup;
                $currentDataSize = strlen($this->getSerializer()->serialize($bunchRows));
                $startNewBunch = false;
                $nextRowBackup = [];
            }
            if ($source->valid()) {
                try {
                    $rowData = $source->current();
                    $isCached = $this->_parameters['cache_products'] ?? false;
                    if ($isCached) {
                        $currentRowHash = sha1(trim(implode('', $rowData)));
                        $cache = $this->cache->load($currentRowHash);
                        if ($cache) {
                            $this->addLogWriteln(
                                __('Product %1 has not changed', $rowData['sku']),
                                $this->output,
                                'info'
                            );
                            $source->next();
                            continue;
                        }
                        $this->originalImportRows[strtolower($rowData['sku'])][] = $currentRowHash;
                    }
                    if (array_key_exists('sku', $rowData)) {
                        $skuSet[$rowData['sku']] = true;
                    }
                    $invalidAttr = [];
                    foreach ($rowData as $attrName => $element) {
                        if (is_string($element)) {
                            if (!mb_check_encoding($element, 'UTF-8')) {
                                unset($rowData[$attrName]);
                                $invalidAttr[] = $attrName;
                            }
                        }
                    }
                    if (!empty($invalidAttr)) {
                        $this->addRowError(
                            AbstractEntity::ERROR_CODE_ILLEGAL_CHARACTERS,
                            $this->_processedRowsCount,
                            implode(',', $invalidAttr)
                        );
                    }
                } catch (InvalidArgumentException $e) {
                    $this->addRowError($e->getMessage(), $this->_processedRowsCount);
                    $this->_processedRowsCount++;
                    $source->next();
                    continue;
                }
                $rowData = array_map('trim', $rowData);
                if (isset($rowData['configurable_variations']) && $rowData['configurable_variations']) {
                    $this->checkAttributePresenceInAttributeSet($rowData);
                }
                $rowData[self::COL_SKU] = $this->getCorrectSkuAsPerLength($rowData);
                $_currentRowSkus[] = mb_strtolower($rowData[self::COL_SKU]);
                $rowData = $this->customFieldsMapping($rowData);
                $rowData = $this->customBunchesData($rowData);

                $this->_processedRowsCount++;

                if ($this->onlyUpdate || $this->onlyAdd) {
                    $oldSkus = $this->skuProcessor->getOldSkus();
                    $productSku = strtolower($this->getCorrectSkuAsPerLength($rowData));

                    if (!isset($oldSkus[$productSku]) && $this->onlyUpdate) {
                        $source->next();
                        continue;
                    } elseif (isset($oldSkus[$productSku]) && $this->onlyAdd) {
                        $source->next();
                        continue;
                    }
                }

                if ($this->onlyUpdate) {
                    foreach ($rowData as $key => $value) {
                        if ('' == $value) {
                            unset($rowData[$key]);
                        }
                    }
                }

                if ($this->getBehavior() == Import::BEHAVIOR_REPLACE) {
                    if (isset($rowData['attribute_set_code'])) {
                        $rowData['_attribute_set'] = $rowData['attribute_set_code'];
                    }
                }
                $rowData = $this->processUrlKey($rowData);
                /* specify url_key for to avoid product repository load
                in \Magento\CatalogUrlRewrite\Observer\AfterImportDataObserver*/
                if (empty($rowData[self::URL_KEY])) {
                    $rowData[self::URL_KEY] = $this->onlyUpdate ? '' : $this->getProductUrlKey($rowData);
                }

                if ($this->validateRow($rowData, $source->key())) {
                    // add row to bunch for save
                    $rowData = $this->_prepareRowForDb($rowData);
                    $rowSize = strlen($this->getSerializer()->serialize($rowData));

                    $isBunchSizeExceeded = $bunchSize > 0 && count($bunchRows) >= $bunchSize;

                    if (!$this->validateRowByProductType($rowData, $source->key())) {
                        $this->addRowError(ValidatorInterface::ERROR_TYPE_UNSUPPORTED, $source->key());
                    }

                    if (($rowData['sku'] !== $this->getLastSku()) &&
                        ($currentDataSize + $rowSize >= $maxDataSize || $isBunchSizeExceeded)) {
                        $startNewBunch = true;
                        $nextRowBackup = [$source->key() => $rowData];
                    } else {
                        $bunchRows[$source->key()] = $rowData;
                        $currentDataSize += $rowSize;
                    }
                    $this->setLastSku($rowData['sku']);
                }

                $source->next();
            }
        }
        if (isset($this->_parameters['disable_products']) && $this->_parameters['disable_products'] > 0) {
            $this->disableProductsNotInList($_currentRowSkus);
        }
        $this->getOptionEntity()->validateAmbiguousData();
        $this->_processedEntitiesCount = (count($skuSet)) ?: $this->_processedRowsCount;

        $this->cache->clean([ImportProductCache::BUFF_CACHE]);
        $this->cache->save(
            $this->getSerializer()->serialize($this->originalImportRows),
            sha1(ImportProductCache::BUFF_CACHE)
        );

        return $this;
    }

    /**
     * @param $rowData
     * @return array
     * @throws Exception
     */
    public function processUrlKey($rowData)
    {
        if ($this->isSkuExist($rowData[self::COL_SKU]) && !isset($rowData[self::URL_KEY])) {
            $rowData[self::URL_KEY] = $this->urlKeyManager
                ->getUrlKeyForSku($rowData[self::COL_SKU]);
        }
        if ($this->_parameters['generate_url'] == 1
            && $this->isNeedToChangeUrlKey($rowData)
        ) {
            $rowData = $this->generateUrlKey($rowData);
        }
        return $rowData;
    }

    /**
     * @param array $_currentRowSkus
     * @throws Exception
     */
    public function disableProductsNotInList(array $_currentRowSkus)
    {
        $attributeCode = $this->scopeConfig->getValue('firebear_importexport/general/supplier_code');
        $supplierValue = $this->_parameters['product_supplier'] ?? null;
        $this->addLogWriteln(__('Disable Products Not in file'), $this->getOutput(), 'info');
        $productIds = [];
        $_updateData = [
            'status' => Status::STATUS_DISABLED,
        ];
        if ($attributeCode && $supplierValue) {
            /** @var Collection $nonExistingProducts */
            $nonExistingProducts = $this->productCollectionFactory->create()
                ->addAttributeToSelect([$this->getProductIdentifierField(), self::COL_SKU, 'status', $attributeCode])
                ->addAttributeToFilter('status', ['neq' => Status::STATUS_DISABLED])
                ->addAttributeToFilter($attributeCode, $supplierValue)
                ->addFieldToFilter('sku', ['nin' => $_currentRowSkus])
                ->load();
            /** @var \Magento\Catalog\Model\Product $nonExistingProduct */
            foreach ($nonExistingProducts as $nonExistingProduct) {
                $this->addLogWriteln(
                    __('Disable Product %1', $nonExistingProduct->getSku()),
                    $this->getOutput(),
                    'info'
                );
                $productIds[] = $nonExistingProduct->getData($this->getProductIdentifierField());
            }
        } else {
            $existingSkus = array_keys($this->getOldSku());
            $nonExistingSkus = array_diff($existingSkus, $_currentRowSkus);
            if (!empty($nonExistingSkus)) {
                foreach ($nonExistingSkus as $sku) {
                    if ($exist = $this->getExistingSku($sku)) {
                        $this->addLogWriteln(__('Disable Product %1', $sku), $this->getOutput(), 'info');
                        $productIds[] = $exist[$this->getProductIdentifierField()];
                    }
                }
            }
        }
        try {
            if (!empty($productIds)) {
                /** @var Action $productAction */
                $productAction = $this->productActionFactory->create();
                foreach ($this->getStoreIds() as $storeId) {
                    $productAction->updateAttributes($productIds, $_updateData, $storeId);
                }
            }
        } catch (Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->getOutput(), 'error');
        }
    }

    /**
     * Checking attribute presence in attribute set
     *
     * @param $rowData
     */
    protected function checkAttributePresenceInAttributeSet($rowData)
    {
        $attributeCodes = [];
        $allAttributesInAttributeSet = [];
        $variations = explode(self::PSEUDO_MULTI_LINE_SEPARATOR, $rowData['configurable_variations']);
        $select = $this->_connection->select()
            ->from($this->getResource()->getTable('eav_attribute_set'))
            ->reset(Zend_Db_Select::COLUMNS)
            ->columns('attribute_set_id')
            ->where('attribute_set_name=(?)', $rowData['attribute_set_code'])
            ->where('entity_type_id=(?)', $this->_entityTypeId);
        $attributeSetId = $this->_connection->fetchRow($select);
        foreach ($variations as $variation) {
            $fieldAndValuePairsText = explode($this->getMultipleValueSeparator(), $variation);
            foreach ($fieldAndValuePairsText as $nameAndValue) {
                $nameAndValue = explode(self::PAIR_NAME_VALUE_SEPARATOR, $nameAndValue);
                if (!empty($nameAndValue)) {
                    $attributeCodes[] = trim($nameAndValue[0]);
                }
            }
        }
        $attributeCodes = array_unique($attributeCodes);
        $select = $this->_connection->select()
            ->from(['eea' => $this->getResource()->getTable('eav_entity_attribute')])
            ->join(
                ['ea' => $this->getResource()->getTable('eav_attribute')],
                'eea.' . 'attribute_id' . ' = ea.' . 'attribute_id'
            )
            ->reset(Zend_Db_Select::COLUMNS)
            ->columns('ea.attribute_code')
            ->where('eea.attribute_set_id=?', $attributeSetId['attribute_set_id'])
            ->where('eea.entity_type_id=?', $this->_entityTypeId);
        $attributeCodesInAttributeSet = array_values($this->_connection->fetchAll($select));
        foreach ($attributeCodesInAttributeSet as $attrCode) {
            $allAttributesInAttributeSet[] = $attrCode['attribute_code'];
        }
        foreach ($attributeCodes as $attributeCode) {
            if ($attributeCode == 'default') {
                continue;
            }
            if (!in_array($attributeCode, $allAttributesInAttributeSet)) {
                $this->addLogWriteln(
                    __(
                        "Not all products can be attached to a configurable product with sku = '%1',
                        since attribute '%2' is missing in attribute set '%3'.",
                        $rowData['sku'],
                        $attributeCode,
                        $rowData['attribute_set_code']
                    ),
                    $this->output,
                    'warning'
                );
            }
        }
    }

    /**
     * @param $sku
     *
     */
    public function setLastSku($sku)
    {
        $this->lastSku = $sku;
    }

    /**
     * @return mixed
     */
    public function getLastSku()
    {
        return $this->lastSku;
    }

    /**
     * @return string[]
     */
    public function getSpecialAttributes()
    {
        return $this->_specialAttributes;
    }

    /**
     * @return array
     */
    public function getAddFields()
    {
        return $this->addFields;
    }

    /**
     * @param string $productSku
     *
     * @return array
     */
    public function getProductWebsites($productSku)
    {
        return isset($this->websitesCache[$productSku]) ? array_keys($this->websitesCache[$productSku]) : [];
    }

    /**
     * @param string $productSku
     *
     * @return array
     */
    public function getProductCategories($productSku)
    {
        return isset($this->categoriesCache[$productSku]) ? array_keys($this->categoriesCache[$productSku]) : [];
    }

    /**
     * @return array
     */
    public function getNotValidSkus()
    {
        return $this->notValidedSku;
    }

    public function setErrorMessages()
    {
        $this->_initErrorTemplates();
    }

    /**
     * @return AbstractType
     */
    public function getSourceType()
    {
        return $this->sourceType;
    }

    /**
     * @return ProductMetadataInterface
     */
    public function getProductMetadata()
    {
        return $this->productMetadata;
    }

    /**
     * Parse values of multiselect attributes depends on "Fields Enclosure" parameter
     *
     * @param string $values
     * @param string $delimiter
     *
     * @return array
     * @since 100.1.2
     */
    public function parseMultiselectValues($values, $delimiter = self::PSEUDO_MULTI_LINE_SEPARATOR)
    {
        if ($delimiter == self::PSEUDO_MULTI_LINE_SEPARATOR
            && isset($this->_parameters['_import_multiple_value_separator'])
            && $this->_parameters['_import_multiple_value_separator']
        ) {
            $delimiter = $this->_parameters['_import_multiple_value_separator'];
        }

        $values = parent::parseMultiselectValues($values, $delimiter);

        if (is_array($values) && !empty($values)) {
            $values = array_map('trim', $values);
        }

        return array_unique(array_filter($values));
    }

    /**
     * @return mixed
     */
    public function getIsRowCategoryMapped()
    {
        return $this->_isRowCategoryMapped;
    }

    /**
     * @param mixed $isRowCategoryMapped
     */
    public function setIsRowCategoryMapped($isRowCategoryMapped)
    {
        $this->_isRowCategoryMapped = $isRowCategoryMapped;
    }

    /**
     * Retrieving images from all columns and rows
     *
     * @param $bunch
     *
     * @return array
     */
    protected function getBunchImages(
        $bunch
    ) {
        $allImagesFromBunch = [];
        foreach ($bunch as $rowData) {
            $rowData = $this->customFieldsMapping($rowData);
            foreach ($this->_imagesArrayKeys as $image) {
                if (empty($rowData[$image])) {
                    continue;
                }
                $dispersionPath =
                    \Magento\Framework\File\Uploader::getDispretionPath($rowData[$image]);
                $importImages = explode($this->getMultipleValueSeparator(), $rowData[$image]);
                foreach ($importImages as $importImage) {
                    $imageSting = mb_strtolower(
                        $dispersionPath . '/' . preg_replace('/[^a-z0-9\._-]+/i', '', $importImage)
                    );
                    /**
                     * TODO: check source type 'file'.
                     * Compare code with default Magento\CatalogImportExport\Model\Import\Product
                     */
                    if (isset($this->_parameters['import_source']) && $this->_parameters['import_source'] != 'file') {
                        $allImagesFromBunch[$this->sourceType->getCode() . $imageSting] = $imageSting;
                    } else {
                        $allImagesFromBunch[$importImage] = $imageSting;
                    }
                }
            }
        }

        return $allImagesFromBunch;
    }

    /**
     * @param $rowData
     *
     * @return mixed
     */
    protected function adjustBundleTypeAttributes($rowData)
    {
        if (isset($rowData['product_type']) && $rowData['product_type'] == 'bundle') {
            $fields = ['price_type', 'weight_type', 'sku_type'];
            foreach ($fields as $field) {
                if (isset($rowData[$field]) && (is_int($rowData[$field]) ||
                        in_array(
                            $rowData[$field],
                            [BundlePrice::PRICE_TYPE_DYNAMIC, BundlePrice::PRICE_TYPE_FIXED]
                        ))
                ) {
                    if ($rowData[$field] === Bundle::VALUE_DYNAMIC) {
                        $rowData[$field] = Bundle::VALUE_DYNAMIC;
                    } elseif ($rowData[$field] === BundlePrice::PRICE_TYPE_DYNAMIC) {
                        $rowData[$field] = Bundle::VALUE_DYNAMIC;
                    } else {
                        $rowData[$field] = Bundle::VALUE_FIXED;
                    }
                }
            }
        }

        return $rowData;
    }

    /**
     * Obtain scope of the row from row data.
     *
     * @param array $rowData
     *
     * @return int
     */
    public function getRowScope(array $rowData)
    {
        if (empty($rowData[self::COL_STORE])
            || $rowData[self::COL_STORE] == 'default'
        ) {
            return self::SCOPE_DEFAULT;
        }
        return self::SCOPE_STORE;
    }

    /**
     * @param array $rowData
     *
     * @return mixed
     */
    public function getCorrectSkuAsPerLength(array $rowData)
    {
        $sku = (string) $rowData[self::COL_SKU];
        return mb_strlen($sku) > Sku::SKU_MAX_LENGTH ?
            mb_substr($sku, 0, Sku::SKU_MAX_LENGTH) : $sku;
    }

    /**
     * Get product entity link field
     *
     * @return string
     * @throws Exception
     */
    private function getProductEntityLinkField()
    {
        if (!$this->productEntityLinkField) {
            $this->productEntityLinkField = $this->getMetadataPool()
                ->getMetadata(ProductInterface::class)
                ->getLinkField();
        }
        return $this->productEntityLinkField;
    }

    /**
     * Update and insert data in entity table.
     *
     * @param array $entityRowsIn Row for insert
     * @param array $entityRowsUp Row for update
     * @return $this
     * @throws ConnectionException
     * @throws DeadlockException
     * @throws LockWaitException
     * @throws Exception
     */
    public function saveProductEntity(array $entityRowsIn, array $entityRowsUp)
    {
        static $entityTable = null;

        if (!$entityTable) {
            $entityTable = $this->getResource()->getEntityTable();
        }
        if ($entityRowsUp) {
            $this->countItemsUpdated += count($entityRowsUp);

            $triesCount = 0;
            do {
                $retry = false;
                try {
                    $this->_connection->insertOnDuplicate(
                        $entityTable,
                        $entityRowsUp,
                        ['type_id', 'updated_at', 'attribute_set_id']
                    );
                } catch (Exception $e) {
                    if ((($e instanceof ConnectionException) ||
                            ($e instanceof DeadlockException) ||
                            ($e instanceof LockWaitException)) &&
                        $triesCount < self::MAX_DB_RETRIES) {
                        $retry = true;
                        $triesCount++;
                        $this->getLogger()->critical($e);
                        sleep(pow($triesCount, 2));
                    } else {
                        throw $e;
                    }
                }
            } while ($retry);
        }

        $entityRowsUp = [];

        try {
            $this->_connection->beginTransaction();
            parent::saveProductEntity($entityRowsIn, $entityRowsUp);
            $this->_connection->commit();
        } catch (Exception $e) {
            $this->_connection->rollBack();
            throw $e;
        }

        return $this;
    }

    /**
     * Validate data row.
     *
     * @param array $rowData
     * @param int $rowNum
     *
     * @return boolean
     */
    public function validateRowByProductType($rowData, $rowNum)
    {
        $sku = $rowData[self::COL_SKU];
        $newSku = $this->skuProcessor->getNewSku($sku);

        if (!isset($this->_productTypeModels[$rowData[self::COL_TYPE]])) {
            $this->addLogWriteln(
                __('Product type is not supported %1', $rowData[self::COL_TYPE]),
                $this->output,
                'error'
            );
            return false;
        }
        if ($newSku && ($newSku['type_id'] !== $rowData[self::COL_TYPE])) {
            $productTypeValidator = $this->_productTypeModels[$rowData[self::COL_TYPE]];
            $productTypeValidator->isRowValid(
                $rowData,
                $rowNum,
                !($this->isSkuExist($rowData[self::COL_SKU]) && Import::BEHAVIOR_REPLACE !== $this->getBehavior())
            );
            return !$this->getErrorAggregator()->isRowInvalid($rowNum);
        }
        return true;
    }

    /**
     * Check if product exists for specified SKU
     *
     * @param string $sku
     *
     * @return bool
     */
    private function isSkuExist($sku)
    {
        if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
            $sku = strtolower($sku);
        }
        return isset($this->_oldSku[$sku]);
    }

    /**
     * Get existing product data for specified SKU
     *
     * @param string $sku
     *
     * @return array
     */
    protected function getExistingSku($skuValue)
    {
        $sku = (string)$skuValue;
        if (version_compare($this->productMetadata->getVersion(), '2.2.0', '>=')) {
            $sku = strtolower($sku);
        }
        if (isset($this->_oldSku[$sku])) {
            $result = $this->_oldSku[$sku];
        } else {
            $result = false;
        }
        return $result;
    }

    /**
     * @return $this|MagentoProduct
     * @throws LocalizedException
     * @throws Exception
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     * @SuppressWarnings(PHPMD.NPathComplexity)
     * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
     */
    protected function _saveLinks()
    {
        $resource = $this->_linkFactory->create();
        $mainTable = $resource->getMainTable();
        $positionAttrId = [];
        $nextLinkId = $this->_resourceHelper->getNextAutoincrement($mainTable);

        foreach ($this->_linkNameToId as $linkName => $linkId) {
            $select = $this->_connection->select()->from(
                $resource->getTable('catalog_product_link_attribute'),
                ['id' => 'product_link_attribute_id']
            )->where(
                'link_type_id = :link_id AND product_link_attribute_code = :position'
            );
            $bind = [':link_id' => $linkId, ':position' => 'position'];
            $positionAttrId[$linkId] = $this->_connection->fetchOne($select, $bind);
        }
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $productIds = [];
            $linkRows = [];
            $positionRows = [];

            foreach ($bunch as $rowNum => $rowData) {
                if (!$this->isRowAllowedToImport($rowData, $rowNum)) {
                    continue;
                }

                $sku = $rowData[self::COL_SKU];

                $productId = $this->skuProcessor->getNewSku($sku)[$this->getProductEntityLinkField()];
                $productLinkKeys = [];
                $select = $this->_connection->select()->from(
                    $resource->getTable('catalog_product_link'),
                    ['id' => 'link_id', 'linked_id' => 'linked_product_id', 'link_type_id' => 'link_type_id']
                )->where(
                    'product_id = :product_id'
                );
                $bind = [':product_id' => $productId];
                foreach ($this->_connection->fetchAll($select, $bind) as $linkData) {
                    $linkKey = "{$productId}-{$linkData['linked_id']}-{$linkData['link_type_id']}";
                    $productLinkKeys[$linkKey] = $linkData['id'];
                }
                foreach ($this->_linkNameToId as $linkName => $linkId) {
                    $productIds[] = $productId;
                    if (isset($rowData[$linkName . 'sku'])) {
                        $linkSkus = explode($this->getMultipleValueSeparator(), $rowData[$linkName . 'sku']);
                        $linkPositions = !empty($rowData[$linkName . 'position'])
                            ? explode($this->getMultipleValueSeparator(), $rowData[$linkName . 'position'])
                            : [];
                        foreach ($linkSkus as $linkedSkuKey => $linkedSku) {
                            $linkedSku = trim($linkedSku);
                            if ((($this->skuProcessor->getNewSku($linkedSku) !== null) || $this->isSkuExist($linkedSku))
                                && strcasecmp($linkedSku, $sku) !== 0
                            ) {
                                $newSku = $this->skuProcessor->getNewSku($linkedSku);
                                if (!empty($newSku)) {
                                    $linkedId = $newSku['entity_id'];
                                } else {
                                    $linkedId = $this->getExistingSku($linkedSku)['entity_id'];
                                }

                                if ($linkedId == null) {
                                    $this->getLogger()->critical(
                                        new Exception(
                                            sprintf(
                                                'WARNING: Orphaned link skipped: From SKU %s (ID %d) to SKU %s, ' .
                                                'Link type id: %d',
                                                $sku,
                                                $productId,
                                                $linkedSku,
                                                $linkId
                                            )
                                        )
                                    );
                                    continue;
                                }

                                $linkKey = "{$productId}-{$linkedId}-{$linkId}";
                                if (empty($productLinkKeys[$linkKey])) {
                                    $productLinkKeys[$linkKey] = $nextLinkId;
                                }
                                if (!isset($linkRows[$linkKey])) {
                                    $linkRows[$linkKey] = [
                                        'link_id' => $productLinkKeys[$linkKey],
                                        'product_id' => $productId,
                                        'linked_product_id' => $linkedId,
                                        'link_type_id' => $linkId,
                                    ];
                                }
                                if (!empty($linkPositions[$linkedSkuKey]) &&
                                    $this->isLinkExists($linkRows, $productLinkKeys[$linkKey])) {
                                    $positionRows[] = [
                                        'link_id' => $productLinkKeys[$linkKey],
                                        'product_link_attribute_id' => $positionAttrId[$linkId],
                                        'value' => $linkPositions[$linkedSkuKey],
                                    ];
                                } elseif ($this->isLinkExists($linkRows, $productLinkKeys[$linkKey])) {
                                    $positionRows[] = [
                                        'link_id' => $productLinkKeys[$linkKey],
                                        'product_link_attribute_id' => $positionAttrId[$linkId],
                                        'value' => $linkedSkuKey + 1,
                                    ];
                                }
                                $nextLinkId++;
                            }
                        }
                    }
                }
            }
            if (Import::BEHAVIOR_APPEND != $this->getBehavior() && $productIds) {
                $this->_connection->delete(
                    $mainTable,
                    $this->_connection->quoteInto('product_id IN (?)', array_unique($productIds))
                );
            }
            $this->savePreparedLinks($linkRows, $positionRows);
        }
        return $this;
    }

    /**
     * @param $links
     * @param $linkId
     *
     * @return bool
     */
    private function isLinkExists($links, $linkId)
    {
        foreach ($links as $linkData) {
            if ($linkData['link_id'] == $linkId) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param $linkRows
     * @param $positionRows
     * @throws LocalizedException
     */
    private function savePreparedLinks($linkRows, $positionRows)
    {
        $resource = $this->_linkFactory->create();
        $mainTable = $resource->getMainTable();
        if ($linkRows) {
            $this->_connection->insertOnDuplicate($mainTable, $linkRows, ['link_id']);
        }
        if ($positionRows) {
            $this->_connection->insertOnDuplicate(
                $resource->getAttributeTypeTable('int'),
                $positionRows,
                ['value']
            );
        }
    }

    /**
     * @param $bunchRows
     *
     * @return mixed
     */
    private function prepareCustomOptionRows($bunchRows)
    {
        $notValidRows = [];
        $validRows = [];

        foreach ($bunchRows as $rowNumber => $rowData) {
            if (empty($rowData['store_view_code']) && !empty($rowData['custom_options'])) {
                $validRows[$rowData['sku']] = true;
            } else {
                if (!empty($rowData['store_view_code']) && !empty($rowData['custom_options'])) {
                    if (!in_array($rowData['sku'], array_keys($validRows))) {
                        $notValidRows[] = $rowNumber;
                    }
                }
            }
        }

        $fixedRowData = [];

        if (!empty($notValidRows)) {
            foreach ($notValidRows as $notValidRow) {
                $fixedRow = null;
                if (strpos($bunchRows[$notValidRow]['custom_options'], 'required') !== false) {
                    $fixedRow = $bunchRows[$notValidRow];
                    $fixedRow['store_view_code'] = null;
                }
                if (!isset($fixedRowData[$bunchRows[$notValidRow]['sku']]) && $fixedRow) {
                    $fixedRowData[$bunchRows[$notValidRow]['sku']] = [$notValidRow => $fixedRow];
                }
            }
        }

        if (!empty($fixedRowData)) {
            foreach ($fixedRowData as $sku => $data) {
                foreach ($data as $notValidRow => $fixedRow) {
                    $bunchRows[$notValidRow] = $fixedRow;
                }
            }
        }

        return $bunchRows;
    }

    /**
     * @return array
     */
    protected function getStoreIds()
    {
        $storeIds = array_merge(
            array_keys($this->getStoreManager()->getStores()),
            [0]
        );
        return $storeIds;
    }

    /**
     * @return array
     */
    protected function getStoreWithCodes()
    {
        $stores = [];
        foreach ($this->getStoreManager()->getStores() as $store) {
            $stores[$store->getId()] = $store->getCode();
        }
        return $stores;
    }

    /**
     * Returns attributes all values in label-value or value-value pairs form. Labels are lower-cased.
     *
     * @param AbstractAttribute $attribute
     * @param array $indexValAttrs OPTIONAL Additional attributes' codes with index values.
     *
     * @return array
     */
    public function getAttributeOptions(AbstractAttribute $attribute, $indexValAttrs = [])
    {
        $options = [];
        $stores = $this->getStoreWithCodes();
        $stores[0] = 'admin'; // We add admin store here for backward compatibility
        foreach ($stores as $id => $code) {
            $attribute->setStoreId($id);
            $options[$code] = parent::getAttributeOptions($attribute, $indexValAttrs);
            try {
                if ($this->swatchesHelperData->isTextSwatch($attribute)) {
                    $swatchOptionsIds = array_values($options[$code]);
                    $swatchOption = $this->getSwatchesByOptionsId($swatchOptionsIds, $attribute->getAttributeId());
                    foreach ($swatchOption as $optionId => $optionValue) {
                        if (isset($optionValue['value'])
                            && ($optionValue['value'] !== '' || $optionValue['value'] !== null)
                        ) {
                            $options[$code][strtolower($optionValue['value'])] = $optionId;
                        }
                    }
                }
            } catch (Exception $e) {
                $this->addLogWriteln($e->getMessage(), $this->getOutput(), 'error');
            }
        }

        return $options;
    }

    /**
     * Upload Media Images with additional checks
     *
     * @param $rowData
     * @param $mediaGallery
     * @param $existingImages
     * @param $uploadedImages
     * @param $rowNum
     * @throws LocalizedException
     */
    protected function processMediaGalleryRows(&$rowData, &$mediaGallery, &$existingImages, &$uploadedImages, $rowNum)
    {
        $disabledImages = [];
        $rowSku = $this->getCorrectSkuAsPerLength($rowData);

        $removeImages = $this->_parameters['remove_images'] ?? 0;

        if ($removeImages == 1 && array_key_exists($rowSku, $existingImages)) {
            $this->removeExistingImages($existingImages[$rowSku]);
            unset($existingImages[$rowSku]);
        }

        $rowData = $this->checkAdditionalImages($rowData);

        if ($this->sourceType && $this->sourceType->getCode() === 'rest') {
            unset($rowData['additional_images']);
        }

        if (isset($rowData['image'])) {
            if (!isset($rowData['thumbnail'])) {
                $rowData['thumbnail'] = $rowData['image'];
            }
            if (!isset($rowData['small_image'])) {
                $rowData['small_image'] = $rowData['image'];
            }
        }

        list($rowImages, $rowLabels) = $this->getImagesFromRow($rowData);

        if (isset($rowData['_media_is_disabled'])) {
            $disabledImages = array_flip(
                explode($this->getMultipleValueSeparator(), $rowData['_media_is_disabled'])
            );
        }

        $rowData[self::COL_MEDIA_IMAGE] = [];
        $imagePosition = 0;
        if (isset($existingImages[$rowSku])) {
            $existingProductImages = $existingImages[$rowSku];
            $existingImagesPosition = array_map(
                function ($value) {
                    if (is_array($value)) {
                        return (int) $value['position'] ?? 0;
                    } else {
                        return 0;
                    }
                },
                $existingProductImages
            );
            $imagePosition = max($existingImagesPosition);
        }
        foreach ($rowImages as $column => $columnImages) {
            foreach ($columnImages as $position => $columnImage) {
                list($isAlreadyUploaded, $alreadyUploadedFile) = $this
                    ->checkAlreadyUploadedImages($existingImages, $columnImage, $rowSku);

                if (isset($uploadedImages[$columnImage])) {
                    $uploadedFile = $uploadedImages[$columnImage];
                } elseif ($isAlreadyUploaded) {
                    $uploadedFile = $alreadyUploadedFile;
                } else {
                    $uploadedFile = $this->uploadMediaFiles(trim($columnImage), true);

                    if ($uploadedFile) {
                        $uploadedImages[$columnImage] = $uploadedFile;
                    } else {
                        $this->addRowError(
                            sprintf(__('Wrong URL/path used for attribute %s in rows'), $column),
                            $rowData['rowNum'] ?? $rowNum,
                            null,
                            null,
                            ProcessingError::ERROR_LEVEL_WARNING
                        );
                    }
                }

                if ($uploadedFile && $column !== self::COL_MEDIA_IMAGE) {
                    $rowData[$column] = $uploadedFile;
                }

                $imageNotAssigned = !isset($existingImages[$rowSku][$uploadedFile]);

                if ($uploadedFile && $imageNotAssigned) {
                    if ($column == self::COL_MEDIA_IMAGE) {
                        $rowData[$column][] = $uploadedFile;
                    }
                    $pos = ++$imagePosition;
                    $mediaData = [
                        'attribute_id' => $this->getMediaGalleryAttributeId(),
                        'label' => isset($rowLabels[$column][$position]) ? $rowLabels[$column][$position] : '',
                        'position' => $pos,
                        'disabled' => isset($disabledImages[$columnImage]) ? '1' : '0',
                        'value' => $uploadedFile,
                    ];

                    if ($this->_getUploader()->checkValidUrl($columnImage)) {
                        $mediaData[ProductVideo::VIDEO_URL_COLUMN] = $columnImage;
                    }

                    if (version_compare($this->productMetadata->getVersion(), '2.2.4', '>=')) {
                        $storeIds = $this->getStoreIds();
                        foreach ($storeIds as $storeId) {
                            $mediaGallery[$storeId][$rowSku][] = $mediaData;
                        }
                    } else {
                        $mediaGallery[$rowSku][] = $mediaData;
                    }
                    $existingImages[$rowSku][$uploadedFile] = true;
                }
            }
        }
    }

    /**
     * @return StoreManagerInterface
     */
    public function getStoreManager(): StoreManagerInterface
    {
        return $this->storeManager;
    }

    /**
     * @param $productId
     * @return array
     * @throws Exception
     */
    public function getCategoryLinks($productId)
    {
        $categoryLinkMetadata = $this->getMetadataPool()->getMetadata(CategoryLinkInterface::class);
        $select = $this->_connection->select();
        $select->from($categoryLinkMetadata->getEntityTable(), ['category_id', 'position']);
        $select->where('product_id = ?', $productId);
        $result = $this->_connection->fetchAll($select);
        return $result;
    }

    /**
     * @param $categoryIds
     * @return array
     * @throws Exception
     */
    public function getCategoryPosition($categoryIds)
    {
        $categoryLinkMetadata = $this->getMetadataPool()->getMetadata(CategoryLinkInterface::class);
        $select = $this->_connection->select();
        $select->from($categoryLinkMetadata->getEntityTable(), ['position']);
        $select->where('category_id in (?)', $categoryIds);
        $result = $this->_connection->fetchAll($select);
        return $result;
    }

    /**
     * Whether a url key is needed to be change.
     *
     * @param array $rowData
     * @return bool
     */
    private function isNeedToChangeUrlKey(array $rowData): bool
    {
        $urlKey = $this->getUrlKey($rowData);
        $productExists = $this->isSkuExist($rowData[self::COL_SKU]);
        $markedToEraseUrlKey = isset($rowData[self::URL_KEY]);
        // The product isn't new and the url key index wasn't marked for change.
        if (!$urlKey && $productExists && !$markedToEraseUrlKey) {
            // Seems there is no need to change the url key
            return false;
        }
        return true;
    }

    /**
     * Check Already uploaded images against fileName to avoid upload
     *
     * @param $existingImages
     * @param $columnImage
     * @param $rowSku
     * @return array
     * @throws LocalizedException
     */
    protected function checkAlreadyUploadedImages(&$existingImages, $columnImage, $rowSku)
    {
        return $this->importImageProcessor->checkAlreadyUploadedImages($existingImages, $columnImage, $rowSku);
    }

    /**
     * @param array $mediaGalleryData
     * @return $this|MagentoProduct
     */
    protected function _saveMediaGallery(array $mediaGalleryData)
    {
        if (empty($mediaGalleryData)) {
            return $this;
        }

        if (!class_exists(MediaGalleryProcessor::class)) {
            parent::_saveMediaGallery($mediaGalleryData);
            return $this;
        }

        $this->mediaProcessor->saveMediaGallery($mediaGalleryData);

        return $this;
    }

    /**
     * Get existing images for current bunch
     *
     * @param array $bunch
     * @return array
     */
    protected function getExistingImages($bunch)
    {
        if (!class_exists(MediaGalleryProcessor::class)) {
            return parent::getExistingImages($bunch);
        }
        return $this->mediaProcessor->getExistingImages($bunch);
    }
}
