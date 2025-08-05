<?php
declare(strict_types=1);

namespace Firebear\ImportExport\Plugin\Import;

use Magento\CatalogImportExport\Model\StockItemImporterInterface;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\Module\Manager;
use Magento\Inventory\Model\SourceItem\Command\Handler\SourceItemsSaveHandler;
use Magento\InventoryApi\Api\Data\SourceItemInterface;
use Magento\InventoryApi\Api\Data\SourceItemInterfaceFactory;
use Magento\InventoryCatalogApi\Api\DefaultSourceProviderInterface;

/**
 * Class SourceItemImporter
 * @package Firebear\ImportExport\Plugin\Import
 */
class SourceItemImporter
{
    /**
     * @var Manager
     */
    protected $moduleManager;

    /**
     * Source Item Interface Factory
     *
     * @var SourceItemInterfaceFactory $sourceItemFactory
     */
    private $sourceItemFactory;

    /**
     * Default Source Provider
     *
     * @var DefaultSourceProviderInterface $defaultSource
     */
    private $defaultSource;

    /**
     * @var SourceItemsSaveHandler
     */
    protected $sourceItemsSaveHandler;

    /**
     * SourceItemImporter constructor.
     * @param Manager $moduleManager
     */
    public function __construct(
        Manager $moduleManager
    ) {
        $this->moduleManager = $moduleManager;
    }

    /**
     * @param StockItemImporterInterface $subject
     * @param $result
     * @param array $stockData
     * @return mixed
     */
    public function afterImport(
        StockItemImporterInterface $subject,
        $result,
        array $stockData
    ) {
        if ($this->moduleManager->isEnabled('Magento_Inventory')) {
            if (interface_exists(DefaultSourceProviderInterface::class)) {
                $this->defaultSource = ObjectManager::getInstance()
                    ->get(DefaultSourceProviderInterface::class);
            }
            if (class_exists(SourceItemInterfaceFactory::class)) {
                $this->sourceItemFactory = ObjectManager::getInstance()
                    ->get(SourceItemInterfaceFactory::class);
            }
            if (class_exists(SourceItemsSaveHandler::class)) {
                $this->sourceItemsSaveHandler = ObjectManager::getInstance()
                    ->get(SourceItemsSaveHandler::class);
            }
            $sourceItems = [];
            foreach ($stockData as $sku => $stockDatum) {
                $inStock = (isset($stockDatum['is_in_stock'])) ? ((int)$stockDatum['is_in_stock']) : 0;
                $qty = (isset($stockDatum['qty'])) ? $stockDatum['qty'] : 0;
                /** @var SourceItemInterface $sourceItem */
                $sourceItem = $this->sourceItemFactory->create();
                $sourceItem->setSku((string)$sku);
                $sourceItem->setSourceCode($this->defaultSource->getCode());
                $sourceItem->setQuantity((float)$qty);
                $sourceItem->setStatus($inStock);
                $sourceItems[] = $sourceItem;
            }
            if (count($sourceItems) > 0) {
                /** SourceItemInterface[] $sourceItems */
                $this->sourceItemsSaveHandler->execute($sourceItems);
            }
        }
        return $result;
    }
}
