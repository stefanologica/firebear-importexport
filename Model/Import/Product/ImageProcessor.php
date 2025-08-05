<?php

namespace Firebear\ImportExport\Model\Import\Product;

use Magento\Catalog\Model\Product\Attribute\Backend\Sku;
use Magento\CatalogImportExport\Model\Import\Product\MediaGalleryProcessor;
use Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModelFactory;
use Magento\CatalogImportExport\Model\Import\Product;
use Magento\Store\Model\StoreManagerInterface;
use Magento\Framework\App\ProductMetadataInterface;
use Firebear\ImportExport\Model\QueueMessage\ImagePublisher;
use Magento\Framework\Serialize\SerializerInterface;
use Magento\ImportExport\Model\Import;
use Magento\Framework\App\Filesystem\DirectoryList;
use Firebear\ImportExport\Model\Import\UploaderFactory;
use Magento\Framework\Exception\LocalizedException;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingError;
use Symfony\Component\Console\Output\ConsoleOutput;
use Magento\Framework\Filesystem;
use Magento\CatalogImportExport\Model\Import\Product\ImageTypeProcessor;
use Psr\Log\LoggerInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Firebear\ImportExport\Traits\General;
use Magento\Catalog\Model\Product\Media\ConfigInterface as MediaConfig;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\EntityManager\MetadataPool;
use Magento\Catalog\Api\Data\ProductInterface;

/**
 * Class ImageProcessor
 * @package Firebear\ImportExport\Model\Import\Product
 */
class ImageProcessor
{
    use General;

    protected $mediaGalleryProcessor;

    protected $mediaGalleryAttributeId;

    protected $resourceModelFactory;

    /**
     * @var StoreManagerInterface
     */
    protected $storeManager;

    /**
     * @var ProductMetadataInterface
     */
    protected $productMetadata;

    /**
     * @var ImagePublisher
     */
    protected $imagePublisher;

    /**
     * @var array
     */
    protected $rows = [];

    /**
     * @var array
     */
    protected $config = [];

    /**
     * @var SerializerInterface
     */
    protected $serializer;

    protected $mediaGalleryTableName;

    protected $resource;

    protected $imagesArrayKeys = ['image', 'small_image', 'thumbnail', 'swatch_image', '_media_image'];

    protected $fileUploader;

    protected $uploaderFactory;

    /**
     * @var MediaConfig
     */
    protected $mediaConfig;

    protected $imageConfigField = ['image', 'small_image', 'thumbnail'];

    /**
     * @var LoggerInterface
     */
    protected $_logger;

    /**
     * @var array
     */
    protected $attrIds = [];

    protected $attrTypeId;

    /**
     * @var ProcessingErrorAggregatorInterface
     */
    protected $errorAggregator;

    /**
     * @var \Magento\Framework\Filesystem\Directory\WriteInterface
     */
    protected $mediaDirectory;

    /**
     * @var ImageTypeProcessor
     */
    protected $imageTypeProcessor;

    protected $productEntityTableName;

    protected $mediaGalleryValueTableName;

    protected $mediaGalleryEntityToValueTableName;

    protected $productEntityLinkField;

    /**
     * @var MetadataPool
     */
    protected $metadataPool;

    /**
     * ImageProcessor constructor.
     * @param Filesystem $filesystem
     * @param LoggerInterface $logger
     * @param StoreManagerInterface $storeManager
     * @param SerializerInterface $serializer
     * @param UploaderFactory $uploaderFactory
     * @param ResourceModelFactory $resourceModelFactory
     * @param ImagePublisher $imagePublisher
     * @param ProductMetadataInterface $productMetadata
     * @param ProcessingErrorAggregatorInterface $errorAggregator
     * @param MediaConfig $mediaConfig
     * @param MetadataPool $metadataPool
     * @param ConsoleOutput $output
     * @throws \Magento\Framework\Exception\FileSystemException
     */
    public function __construct(
        Filesystem $filesystem,
        LoggerInterface $logger,
        StoreManagerInterface $storeManager,
        SerializerInterface $serializer,
        UploaderFactory $uploaderFactory,
        ResourceModelFactory $resourceModelFactory,
        ImagePublisher $imagePublisher,
        ProductMetadataInterface $productMetadata,
        ProcessingErrorAggregatorInterface $errorAggregator,
        MediaConfig $mediaConfig,
        MetadataPool $metadataPool,
        ConsoleOutput $output
    ) {
        $this->storeManager = $storeManager;
        $this->serializer = $serializer;
        $this->productMetadata = $productMetadata;
        $this->imagePublisher = $imagePublisher;
        $this->resourceModelFactory = $resourceModelFactory;
        $this->uploaderFactory = $uploaderFactory;
        $this->mediaDirectory = $filesystem->getDirectoryWrite(DirectoryList::ROOT);
        $this->output = $output;
        $this->_logger = $logger;
        $this->errorAggregator = $errorAggregator;
        $this->mediaConfig = $mediaConfig;
        $this->metadataPool = $metadataPool;
        if (class_exists(ImageTypeProcessor::class)) {
            $this->imageTypeProcessor = ObjectManager::getInstance()
                ->get(ImageTypeProcessor::class);
        }
        if (class_exists(MediaGalleryProcessor::class)) {
            $this->mediaGalleryProcessor = ObjectManager::getInstance()
                ->get(MediaVideoGallery::class);
        }
    }

    /**
     * @param $rowData
     * @param $mediaGallery
     * @param $existingImages
     * @param $uploadedImages
     * @param $rowNum
     * @throws LocalizedException
     */
    public function processMediaGalleryRows(&$rowData, &$mediaGallery, &$existingImages, &$uploadedImages, $rowNum)
    {
        $disabledImages = [];
        $rowSku = $this->getCorrectSkuAsPerLength($rowData);
        $removeImages = $this->config['remove_images'] ?? 0;

        if ($removeImages == 1 && array_key_exists($rowSku, $existingImages)) {
            $this->removeExistingImages($existingImages[$rowSku]);
            unset($existingImages[$rowSku]);
        }

        $rowData = $this->checkAdditionalImages($rowData);
        if (isset($this->config['source_type']) && $this->config['source_type'] === 'rest') {
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

        $rowData[Product::COL_MEDIA_IMAGE] = [];
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

                if ($uploadedFile && $column !== Product::COL_MEDIA_IMAGE) {
                    $rowData[$column] = $uploadedFile;
                }

                $imageNotAssigned = !isset($existingImages[$rowSku][$uploadedFile]);

                if ($uploadedFile && $imageNotAssigned) {
                    if ($column == Product::COL_MEDIA_IMAGE) {
                        $rowData[$column][] = $uploadedFile;
                    }

                    $mediaData = [
                        'attribute_id' => $this->getMediaGalleryAttributeId(),
                        'label' => isset($rowLabels[$column][$position]) ? $rowLabels[$column][$position] : '',
                        'position' => $position + 1,
                        'disabled' => isset($disabledImages[$columnImage]) ? '1' : '0',
                        'value' => $uploadedFile,
                    ];

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
     * @param $conf
     * @return $this
     */
    public function setConfig($conf)
    {
        $this->config = $conf;
        return $this;
    }

    /**
     * @return mixed|string
     */
    public function getMultipleValueSeparator()
    {
        if (!empty($this->config[Import::FIELD_FIELD_MULTIPLE_VALUE_SEPARATOR])) {
            return $this->config[Import::FIELD_FIELD_MULTIPLE_VALUE_SEPARATOR];
        }
        return Import::DEFAULT_GLOBAL_MULTI_VALUE_SEPARATOR;
    }

    /**
     * @param $message
     * @throws LocalizedException
     */
    public function processImportImages($message)
    {
        $message = $this->serializer->unserialize($message);
        $this->config = $message['config'];
        $rows = $message['data'];
        $mediaGallery = $uploadedImages = [];
        $existingImages = $this->getExistingImages($rows);

        foreach ($rows as $rowNum => &$rowData) {
            $this->processMediaGalleryRows(
                $rowData,
                $mediaGallery,
                $existingImages,
                $uploadedImages,
                $rowNum
            );
        }

        $this->saveMediaGallery($mediaGallery);
        if (!empty($this->config['deferred_images'])) {
            $this->saveImageConfig($rows);
        }

        $this->mediaGalleryProcessor->resetIdBySku();
    }

    /**
     * @param $rows
     * @throws \Exception
     */
    protected function saveImageConfig($rows)
    {
        $insertBunch = [];
        $this->mediaGalleryProcessor->initDataQueue($rows);
        foreach ($this->getStoreIds() as $storeId) {
            foreach ($rows as $row) {
                foreach ($this->imageConfigField as $field) {
                    if (isset($row[$field])) {
                        $insertBunch[] = [
                            'attribute_id' => $this->getAttributeIdByCode($field),
                            $this->getProductEntityLinkField() => $this->mediaGalleryProcessor->getIdBySku($row['sku']),
                            'value' => $row[$field],
                            'store_id' => $storeId
                        ];
                    }
                }
            }
        }

        if ($insertBunch) {
            $conn = $this->getResource()->getConnection();
            $conn->insertOnDuplicate(
                $conn->getTableName('catalog_product_entity_varchar'),
                $insertBunch
            );
        }
    }

    /**
     * @param $code
     * @return mixed
     */
    protected function getAttributeIdByCode($code)
    {
        if (isset($this->attrIds[$code])) {
            return $this->attrIds[$code];
        }

        $connection = $this->getResource()->getConnection();
        $query = $connection->select()
            ->from($connection->getTableName('eav_attribute'), 'attribute_id')
            ->where(
                "attribute_code = '{$code}' AND entity_type_id = ?",
                $this->getAttributeTypeCode()
            );

        $this->attrIds[$code] = $connection->fetchOne($query);
        return $this->attrIds[$code];
    }

    /**
     * @return string
     */
    protected function getAttributeTypeCode()
    {
        if ($this->attrTypeId) {
            return $this->attrTypeId;
        }

        $connection = $this->getResource()->getConnection();
        $query = $connection->select()
            ->from(['st' => $connection->getTableName('eav_entity_type')])
            ->where('st.entity_type_code = ?', ProductAttributeInterface::ENTITY_TYPE_CODE);
        $this->attrTypeId = $connection->fetchOne($query);

        return $this->attrTypeId;
    }

    /**
     * Get existing images for current bunch
     *
     * @param array $bunch
     * @return array
     */
    protected function getExistingImages($bunch)
    {
        return $this->mediaGalleryProcessor->getExistingImages($bunch);
    }

    /**
     * @param array $mediaGalleryData
     * @return $this
     */
    protected function saveMediaGallery(array $mediaGalleryData)
    {
        if (empty($mediaGalleryData)) {
            return $this;
        }

        if (!empty($this->config['deferred_images'])) {
            $this->mediaGalleryProcessor->initDataQueue($mediaGalleryData);
        }

        $this->mediaGalleryProcessor->saveMediaGallery($mediaGalleryData);

        return $this;
    }

    /**
     * @return array
     */
    protected function getStoreIds()
    {
        $storeIds = array_merge(
            array_keys($this->storeManager->getStores()),
            [0]
        );
        return $storeIds;
    }

    /**
     * @param $newMediaValues
     * @return $this
     */
    public function removeExistingImages($newMediaValues)
    {
        try {
            $this->initMediaGalleryResources();
            if (isset($this->config['remove_images_dir'])
                && $this->config['remove_images_dir'] == 1
            ) {
                foreach ($newMediaValues as $newMediaValue) {
                    $mediaPath = DirectoryList::PUB . DIRECTORY_SEPARATOR . DirectoryList::MEDIA .
                        DIRECTORY_SEPARATOR . $this->mediaConfig->getMediaPath($newMediaValue['value']);
                    $productImage = $this->mediaDirectory
                        ->getAbsolutePath($mediaPath);
                    if ($this->mediaDirectory->isExist($productImage)) {
                        $this->addLogWriteln(
                            __('Remove Image for Product %1 from media directory', $newMediaValue[Product::COL_SKU]),
                            $this->getOutput(),
                            'info'
                        );
                        $this->mediaDirectory->delete($productImage);
                    }
                }
            }
            $connection = $this->getResource()->getConnection();
            $connection->delete(
                $this->mediaGalleryTableName,
                $connection->quoteInto('value_id IN (?)', $newMediaValues)
            );
        } catch (\Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->getOutput(), 'error');
        }

        return $this;
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
     * @return \Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModel
     */
    protected function getResource()
    {
        if (!$this->resource) {
            $this->resource = $this->resourceModelFactory->create();
        }
        return $this->resource;
    }

    /**
     * Divide additionalImages for old Magento version
     * @param $rowData
     *
     * @return mixed
     */
    public function checkAdditionalImages($rowData)
    {
        if (version_compare($this->productMetadata->getVersion(), '2.1.11', '<')) {
            $newImage = [];
            if (isset($rowData['additional_images'])) {
                $importImages = explode($this->getMultipleValueSeparator(), $rowData['additional_images']);
                $newImage = $importImages;
            }
            if (!empty($newImage)) {
                $rowData['additional_images'] = implode(',', $newImage);
            }
        }
        return $rowData;
    }

    /**
     * @param array $rowData
     * @return array
     */
    public function getImagesFromRow(array $rowData)
    {
        $images = [];
        $labels = [];
        foreach ($this->imagesArrayKeys as $column) {
            if (!empty($rowData[$column])) {
                $images[$column] = array_unique(
                    array_map(
                        'trim',
                        explode($this->getMultipleValueSeparator(), $rowData[$column])
                    )
                );

                if (!empty($rowData[$column . '_label'])) {
                    $labels[$column] = $this->parseMultipleValues($rowData[$column . '_label']);

                    if (count($labels[$column]) > count($images[$column])) {
                        $labels[$column] = array_slice($labels[$column], 0, count($images[$column]));
                    }
                }
            }
        }

        return [$images, $labels];
    }

    /**
     * Parse values from multiple attributes fields
     *
     * @param string $labelRow
     * @return array
     */
    private function parseMultipleValues($labelRow)
    {
        return $this->parseMultiselectValues(
            $labelRow,
            $this->getMultipleValueSeparator()
        );
    }

    /**
     * @param $values
     * @param string $delimiter
     * @return array
     */
    public function parseMultiselectValues($values, $delimiter = Product::PSEUDO_MULTI_LINE_SEPARATOR)
    {
        if (empty($this->config[Import::FIELDS_ENCLOSURE])) {
            return explode($delimiter, $values);
        }
        if (preg_match_all('~"((?:[^"]|"")*)"~', $values, $matches)) {
            return $values = array_map(
                function ($value) {
                    return str_replace('""', '"', $value);
                },
                $matches[1]
            );
        }
        return [$values];
    }

    /**
     * @param array $rowData
     *
     * @return mixed
     */
    public function getCorrectSkuAsPerLength(array $rowData)
    {
        return strlen($rowData[Product::COL_SKU]) > Sku::SKU_MAX_LENGTH ?
            substr($rowData[Product::COL_SKU], 0, Sku::SKU_MAX_LENGTH) : $rowData[Product::COL_SKU];
    }

    /**
     * @return mixed
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function getMediaGalleryAttributeId()
    {
        if (!$this->mediaGalleryAttributeId) {
            /** @var $resource \Magento\CatalogImportExport\Model\Import\Proxy\Product\ResourceModel */
            $resource = $this->resourceModelFactory->create();
            $this->mediaGalleryAttributeId =
                $resource->getAttribute(Product::MEDIA_GALLERY_ATTRIBUTE_CODE)->getId();
        }
        return $this->mediaGalleryAttributeId;
    }

    /**
     * @param $existingImages
     * @param $columnImage
     * @param $rowSku
     * @return array
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function checkAlreadyUploadedImages(&$existingImages, $columnImage, $rowSku)
    {
        $uploadedFileName = hash('sha256', $columnImage);
        $isUploaded = $this->getUploader()::getDispersionPath($uploadedFileName)
            . DIRECTORY_SEPARATOR . $uploadedFileName;
        $isAlreadyUploaded = false;
        $alreadyUploadedFile = '';
        foreach ($this->getUploader()->getAllowedFileExtension() as $fileExtension) {
            $alreadyUploadedFile = $isUploaded . '.' . $fileExtension;
            if (array_key_exists($rowSku, $existingImages)
                && array_key_exists($alreadyUploadedFile, $existingImages[$rowSku])
            ) {
                $isAlreadyUploaded = true;
                break;
            }
        }
        return [$isAlreadyUploaded, $alreadyUploadedFile];
    }

    /**
     * @param string $fileName
     * @param bool $renameFileOff
     * @param array $existingUpload
     *
     * @return string
     */
    public function uploadMediaFiles($fileName, $renameFileOff = false, $existingUpload = [])
    {
        $uploadedFile = '';
        try {
            $result = $this->getUploader()->move($fileName, $renameFileOff, $existingUpload);
            if (!empty($result)) {
                $uploadedFile = $result['file'];
            }
        } catch (\Exception $e) {
            $this->addLogWriteln($e->getMessage(), $this->getOutput(), 'error');
        }
        return $uploadedFile;
    }

    /**
     * @return \Magento\CatalogImportExport\Model\Import\Uploader
     * @throws LocalizedException
     * @throws \Magento\Framework\Exception\FileSystemException
     */
    protected function getUploader()
    {
        $DS = DIRECTORY_SEPARATOR;
        if ($this->fileUploader === null) {
            $this->fileUploader = $this->uploaderFactory->create();
            $this->fileUploader->init();
            $this->fileUploader->setEntity($this);
            $dirConfig = DirectoryList::getDefaultConfig();
            $dirAddon = $dirConfig[DirectoryList::MEDIA][DirectoryList::PATH];
            if (!empty($this->config[Import::FIELD_NAME_IMG_FILE_DIR])) {
                $tmpPath = $this->config[Import::FIELD_NAME_IMG_FILE_DIR];
            } else {
                $tmpPath = $dirAddon . $DS . $this->mediaDirectory->getRelativePath('import');
            }
            if (preg_match('/\bhttps?:\/\//i', $tmpPath, $matches)) {
                $tmpPath = $dirAddon . $DS . $this->mediaDirectory->getRelativePath('import');
            }
            if (!$this->fileUploader->setTmpDir($tmpPath)) {
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
            $destinationPath = $dirAddon . $DS . $this->mediaDirectory->getRelativePath($destinationDir);

            $this->mediaDirectory->create($destinationPath);
            if (!$this->fileUploader->setDestDir($destinationPath)) {
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

        return $this->fileUploader;
    }

    /**
     * @return ProcessingErrorAggregatorInterface
     */
    public function getErrorAggregator()
    {
        return $this->errorAggregator;
    }

    /**
     * Add error with corresponding current data source row number.
     *
     * @param string $errorCode Error code or simply column name
     * @param int $errorRowNum Row number.
     * @param string $colName OPTIONAL Column name.
     * @param string $errorMessage OPTIONAL Column name.
     * @param string $errorLevel
     * @param string $errorDescription
     * @return $this
     */
    public function addRowError(
        $errorCode,
        $errorRowNum,
        $colName = null,
        $errorMessage = null,
        $errorLevel = ProcessingError::ERROR_LEVEL_CRITICAL,
        $errorDescription = null
    ) {
        $errorCode = (string)$errorCode;
        $this->getErrorAggregator()->addError(
            $errorCode,
            $errorLevel,
            $errorRowNum,
            $colName,
            $errorMessage,
            $errorDescription
        );

        return $this;
    }

    /**
     * @return string
     * @throws \Exception
     */
    private function getProductEntityLinkField()
    {
        if (!$this->productEntityLinkField) {
            $this->productEntityLinkField = $this->metadataPool
                ->getMetadata(ProductInterface::class)
                ->getLinkField();
        }
        return $this->productEntityLinkField;
    }
}
