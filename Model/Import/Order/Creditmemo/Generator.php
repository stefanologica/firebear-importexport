<?php
/**
 * @copyright: Copyright © 2020 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */
namespace Firebear\ImportExport\Model\Import\Order\Creditmemo;

use Magento\Framework\DB\TransactionFactory;
use Magento\ImportExport\Model\Import;
use Magento\Sales\Api\OrderRepositoryInterface;
use Magento\Sales\Model\Order\CreditmemoFactory;
use Firebear\ImportExport\Model\Import\Context;
use Firebear\ImportExport\Model\Import\Order\AbstractAdapter;
use Firebear\ImportExport\Model\ResourceModel\Order\Helper;

/**
 * Order Creditmemo Generator
 */
class Generator extends AbstractAdapter
{
    /**
     * Entity Type Code
     */
    const ENTITY_TYPE_CODE = 'order';

    /**
     * Error Codes
     */
    const ERROR_ORDER_ID_IS_EMPTY = 'creditmemoOrderIdIsEmpty';
    const ERROR_ORDER_INCREMENT_ID = 'creditmemoOrderIncrementId';

    /**
     * Validation Failure Message Template Definitions
     *
     * @var array
     */
    protected $_messageTemplates = [
        self::ERROR_ORDER_ID_IS_EMPTY => 'Creditmemo order_id is empty',
        self::ERROR_ORDER_INCREMENT_ID => 'Order with selected increment_id does not exist',
    ];

    /**
     * Order Repository
     *
     * @var OrderRepositoryInterface
     */
    protected $orderRepository;

    /**
     * Creditmemo Factory
     *
     * @var CreditmemoFactory
     */
    protected $creditmemoFactory;

    /**
     * Transaction Factory
     *
     * @var TransactionFactory
     */
    protected $transactionfactory;

    /**
     * Order Creditmemo Table Name
     *
     * @var string
     */
    protected $_mainTable = 'sales_creditmemo';

    /**
     * Initialize Import
     *
     * @param Context $context
     * @param Helper $resourceHelper,
     * @param OrderRepositoryInterface $orderRepository
     * @param CreditmemoFactory $creditmemoFactory
     * @param TransactionFactory $transactionfactory
     */
    public function __construct(
        Context $context,
        Helper $resourceHelper,
        OrderRepositoryInterface $orderRepository,
        CreditmemoFactory $creditmemoFactory,
        TransactionFactory $transactionfactory
    ) {
        $this->orderRepository = $orderRepository;
        $this->creditmemoFactory = $creditmemoFactory;
        $this->transactionfactory = $transactionfactory;

        parent::__construct(
            $context,
            $resourceHelper
        );
    }

    /**
     * Retrieve The Prepared Data
     *
     * @param array $rowData
     * @return array|bool
     */
    public function prepareRowData(array $rowData)
    {
        $this->prepareCurrentOrderId($rowData);
        $rowData = $this->_extractField($rowData, 'creditmemo');
        return (count($rowData) && !$this->isEmptyRow($rowData))
            ? $rowData
            : false;
    }

    /**
     * Is Empty Row
     *
     * @param array $rowData
     * @return bool
     */
    public function isEmptyRow($rowData)
    {
        return parent::isEmptyRow($rowData) && empty($rowData['skus']);
    }

    /**
     * Import Data Rows
     *
     * @return boolean
     */
    protected function _importData()
    {
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            foreach ($bunch as $rowNumber => $rowData) {
                $rowData = $this->prepareRowData($rowData);
                /* validate data */
                if (!$rowData || !$this->validateRow($rowData, $rowNumber)) {
                    continue;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNumber);
                    continue;
                }

                /* behavior selector */
                switch ($this->getBehavior()) {
                    case Import::BEHAVIOR_REPLACE:
                    case Import::BEHAVIOR_ADD_UPDATE:
                        $this->generate($rowData);
                        break;
                }
            }
        }
        return true;
    }

    /**
     * Generate Creditmemo
     *
     * @param array $rowData
     * @return void
     */
    protected function generate(array $rowData)
    {
        $items = [];
        $data = $this->getSkus($rowData['skus']);
        $order = $this->orderRepository->get(
            $this->_currentOrderId
        );

        foreach ($order->getAllVisibleItems() as $item) {
            if (!isset($data[$item->getSku()])) {
                continue;
            }

            if ($item->canRefund()) {
                $qty = min($data[$item->getSku()], $item->getQtyToRefund());
                if (0 < $qty) {
                    $items[$item->getId()] = $qty;
                }
            }
        }

        /* create creditmemo */
        if (0 < count($items)) {
            $creditmemo = $this->creditmemoFactory->createByOrder($order, $items);
            if ($creditmemo->getTotalQty()) {
                $transaction = $this->transactionfactory->create();
                $transaction->addObject(
                    $creditmemo
                )->addObject(
                    $order
                )->save();
            }
        }
    }

    /**
     * Prepare Data For Update
     *
     * @param array $rowData
     * @return array
     */
    protected function _prepareDataForUpdate(array $rowData)
    {
        return $rowData;
    }

    /**
     * Retrieve item skus
     *
     * @param string $skus
     * @return array
     */
    protected function getSkus($skus)
    {
        $data = [];
        foreach (explode(';', $skus) as $row) {
            list($sku, $qty) = explode(':', $row);
            $data[$sku] = $qty;
        }
        return $data;
    }

    /**
     * Retrieve All Fields Source
     *
     * @return array
     */
    public function getAllFields()
    {
        return [];
    }

    /**
     * Validate Data Row
     *
     * @param array $rowData
     * @param int $rowNumber
     * @return boolean
     */
    public function validateRow(array $rowData, $rowNumber)
    {
        if (isset($this->_validatedRows[$rowNumber])) {
            // check that row is already validated
            return !$this->getErrorAggregator()->isRowInvalid($rowNumber);
        }
        $this->_validatedRows[$rowNumber] = true;
        $this->_processedEntitiesCount++;
        /* behavior selector */
        switch ($this->getBehavior()) {
            case Import::BEHAVIOR_REPLACE:
            case Import::BEHAVIOR_ADD_UPDATE:
                $this->_validateRowForUpdate($rowData, $rowNumber);
                break;
        }
        return !$this->getErrorAggregator()->isRowInvalid($rowNumber);
    }

    /**
     * Validate Row Data For Add/Update Behaviour
     *
     * @param array $rowData
     * @param int $rowNumber
     * @return void
     */
    protected function _validateRowForUpdate(array $rowData, $rowNumber)
    {
        if (!empty($this->_currentOrderId)) {
            /* check there is real order */
            if (!$this->getExistOrderId()) {
                $this->addRowError(self::ERROR_ORDER_INCREMENT_ID, $rowNumber);
            }
        } else {
            $this->addRowError(self::ERROR_ORDER_ID_IS_EMPTY, $rowNumber);
        }
    }

    /**
     * Retrieve Order Id If Order Is Present In Database
     *
     * @return bool|int
     */
    protected function getExistOrderId()
    {
        /** @var $select \Magento\Framework\DB\Select */
        $select = $this->_connection->select();
        $select->from($this->getOrderTable(), 'entity_id')
            ->where('increment_id = ?', $this->_currentOrderId);

        return $this->_connection->fetchOne($select);
    }
}
