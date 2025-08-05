<?php
/**
 * AfterImportDataObserver
 *
 * @copyright Copyright © 2019 Firebear Studio. All rights reserved.
 * @author    Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Observer;

use Magento\Framework\Event\ObserverInterface;
use Magento\MediaStorage\Service\ImageResize;
use Magento\Framework\Event\Observer;
use Firebear\ImportExport\Model\ResourceModel\Catalog\GalleryResize;
use Magento\CatalogImportExport\Model\Import\Product as ImportProduct;
use Magento\Framework\Filesystem\DirectoryList;
use Magento\Framework\App\ObjectManager;

/**
 * Class AfterImportDataObserver
 * @package Firebear\ImportExport\Observer
 */
class AfterImportDataObserver implements ObserverInterface
{
    const URL_KEY_ATTRIBUTE_CODE = 'url_key';

    /**
     * @var array
     */
    protected $import;
    /**
     * @var ImageResize
     */
    private $imageResize;

    /**
     * @var GalleryResize
     */
    protected $galleryResize;

    /**
     * @var DirectoryList
     */
    protected $directoryList;

    /**
     * @var array
     */
    protected $ignoreList = [];

    /**
     * AfterImportDataObserver constructor.
     * @param ImageResize $imageResize
     * @param DirectoryList $directoryList
     * @param GalleryResize $galleryResize
     */
    public function __construct(
        DirectoryList $directoryList,
        GalleryResize $galleryResize
    ) {
        $this->galleryResize = $galleryResize;
        $this->directoryList = $directoryList;
        if (class_exists(ImageResize::class)) {
            $this->imageResize = ObjectManager::getInstance()
                ->get(ImageResize::class);
        }
    }

    /**
     * @param Observer $observer
     */
    public function execute(Observer $observer)
    {
        if (!$this->imageResize) {
            return;
        }
        try {
            $this->import = $observer->getEvent()->getAdapter();
            if ($products = $observer->getEvent()->getBunch()) {
                if (isset($this->import->getParameters()['image_resize'])
                    && $this->import->getParameters()['image_resize']
                ) {
                    $products = $this->prepareProducts($products);
                    $imageProducts = $this->getImageProducts($products);
                    foreach ($imageProducts as $sku => $imageProduct) {
                        if (!$imageProduct) {
                            continue;
                        }
                        foreach ($imageProduct as $path) {
                            if (!in_array($path['value'], $this->ignoreList)) {
                                $this->imageResize->resizeFromImageName($path['value']);
                                $this->ignoreList[] = $path['value'];
                            }
                        }
                        $this->addLogWriteln(__('Resize Image for product %1', $sku), 'info');
                    }
                }
            }
        } catch (\Exception $e) {
            $this->addLogWriteln($e->getMessage(), 'error');
        }
    }

    /**
     * @param array $rowData
     * @return array|null
     */
    private function getProductId(array $rowData)
    {
        $newSku = $this->import->getNewSku($rowData[ImportProduct::COL_SKU]);
        if (empty($newSku) || !isset($newSku['entity_id'])) {
            return null;
        }
        if ($this->import->getRowScope($rowData) == ImportProduct::SCOPE_STORE
            && empty($rowData[self::URL_KEY_ATTRIBUTE_CODE])) {
            return null;
        }
        $rowData['entity_id'] = $newSku['entity_id'];
        return $rowData['entity_id'];
    }

    /**
     * @param $debugData
     * @param null $type
     */
    private function addLogWriteln($debugData, $type = null)
    {
        if (method_exists($this->import, 'addLogWriteln')
            && method_exists($this->import, 'getOutput')
        ) {
            $this->import->addLogWriteln(
                $debugData,
                $this->import->getOutput(),
                $type
            );
        }
    }

    /**
     * @param $rows
     * @return mixed
     */
    protected function prepareProducts($rows)
    {
        foreach ($rows as &$row) {
            $row['entity_id'] = $this->getProductId($row);
        }

        return $rows;
    }

    /**
     * @param $rows
     * @return array
     */
    protected function getImageProducts($rows)
    {
        $result = [];

        $items = $this->galleryResize->getImages($rows);
        foreach ($items as $item) {
            $result[$item['sku']][] = $item;
        }

        return $result;
    }

    /**
     * @param $path
     * @return string
     * @throws \Magento\Framework\Exception\FileSystemException
     */
    protected function convertPath($path)
    {
        return $this->directoryList->getPath('media') . '/catalog/product' . $path;
    }
}
