<?php
/**
 * @copyright: Copyright © 2018 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Firebear\ImportExport\Model\ExportFactory;
use Firebear\ImportExport\Ui\Component\Listing\Column\Entity\Export\Options as EntityOptions;
use Magento\Framework\App\HttpRequestInterface;
use Magento\Framework\Controller\ResultInterface;
use Magento\Framework\Controller\Result\Json as ResultJson;
use Magento\Framework\Exception\LocalizedException;

/**
 * Class Downfields
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Downfields extends JobController
{
    /**
     * @var ExportFactory
     */
    protected $export;

    /**
     * @var EntityOptions
     */
    protected $entity;

    /**
     * Downfields constructor.
     *
     * @param Context $context
     * @param ExportFactory $export
     * @param EntityOptions $entity
     */
    public function __construct(
        Context $context,
        ExportFactory $export,
        EntityOptions $entity
    ) {
        parent::__construct($context);

        $this->export = $export;
        $this->entity = $entity;
    }

    /**
     * @return ResultInterface
     * @throws LocalizedException
     */
    public function execute()
    {
        /** @var ResultJson $resultJson */
        $resultJson = $this->resultFactory->create($this->resultFactory::TYPE_JSON);
        $options  = [];
        /** @var HttpRequestInterface $request */
        $request = $this->getRequest();
        if ($request->isAjax()) {
            $entity = $this->getRequest()->getParam('entity');

            if ($entity) {
                $list = $this->loadList($entity);
                $options = $list[$entity] ?? [];
            }
        }

        return $resultJson->setData($options);
    }

    /**
     * @param string $entity
     * @return array
     * @throws LocalizedException
     */
    protected function loadList($entity)
    {
        $entities = $this->entity->toOptionArray();

        $options  = [];
        foreach ($entities as $item) {
            if (isset($item['fields'])) {
                foreach ($item['fields'] as $entityName => $field) {
                    if ($entity != $entityName) {
                        continue;
                    }
                    $options = $this->prepareFields($item['value']);
                }
            } elseif (isset($item['value']) && $entity == $item['value']) {
                $options = $this->prepareFields($entity);
            }
        }

        return $options;
    }

    /**
     * @param string $entity
     * @return array
     * @throws LocalizedException
     */
    protected function prepareFields($entity)
    {
        $options = [];
        $childs = [];
        $fields = $this->export
            ->create()
            ->setData(['entity' => $entity])
            ->getFields();
        foreach ($fields as $field) {
            if (!isset($field['optgroup-name'])) {
                $childs[] = ['value' => $field, 'label' => $field];
            } else {
                $options[$field['optgroup-name']] = $field['value'];
            }
        }
        if (!isset($options[$entity])) {
            $options[$entity] = $childs;
        }

        return $options;
    }
}
