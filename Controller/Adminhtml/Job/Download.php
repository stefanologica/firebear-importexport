<?php

/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\ResponseInterface;
use Magento\Framework\App\Response\Http\FileFactory;
use Magento\Framework\Component\ComponentRegistrar;
use Magento\Framework\Component\ComponentRegistrarInterface;
use Magento\Framework\Filesystem\Directory\ReadFactory;

/**
 * Action Download
 */
class Download extends Job
{
    const MODULE_DONWLOADER_DIR = 'Downloader';

    /**
     * Module registry
     *
     * @var ComponentRegistrarInterface
     */
    private $componentRegistrar;

    /**
     * @var ReadFactory
     */
    private $readFactory;

    /**
     * @var FileFactory
     */
    private $fileFactory;

    /**
     * Download constructor.
     *
     * @param Context $context
     * @param ComponentRegistrarInterface $componentRegistrar
     * @param ReadFactory $readFactory
     * @param FileFactory $fileFactory
     */
    public function __construct(
        Context $context,
        ComponentRegistrarInterface $componentRegistrar,
        ReadFactory $readFactory,
        FileFactory $fileFactory
    ) {
        parent::__construct($context);

        $this->componentRegistrar = $componentRegistrar;
        $this->readFactory = $readFactory;
        $this->fileFactory = $fileFactory;
    }

    /**
     * Execute action
     *
     * @return void
     */
    public function execute()
    {
        $type = $this->getRequest()->getParam('type');
        $source = $this->getRequest()->getParam('source');
        $entity = $this->getRequest()->getParam('entity');

        switch ($type) {
            case 'magento1':
                $outputFile = $this->getFile('catalog_product_magento1.' . $source);
                break;
            case 'magento1-bundle-products':
                $outputFile = $this->getFile('catalog_bundle_products_magento1.' . $source);
                break;
            case 'magento1-downloadable-products':
                $outputFile = $this->getFile('catalog_downloadable_products_magento1.' . $source);
                break;
            default:
                $outputFile = $this->getFile('catalog_product_magento2.' . $source);
                break;
        }
        $dir         = $this->readFactory->create(
            $this->getDir(
                'Firebear_ImportExport',
                self::MODULE_DONWLOADER_DIR
            )
        );
        return $this->downloadCsv($outputFile);
    }

    /**
     * @param $file
     *
     * @return ResponseInterface
     * @throws \Exception
     */
    protected function downloadCsv($file)
    {
        $contentType = 'application/octet-stream';
        $dir         = $this->readFactory->create(
            $this->getDir(
                'Firebear_ImportExport',
                self::MODULE_DONWLOADER_DIR
            )
        );

        if (!$dir->isFile(basename($file))) {
            $this->messageManager->addErrorMessage(__('File not found.'));
        }

        return $this->fileFactory->create(basename($file), file_get_contents($file), DirectoryList::MEDIA);
    }

    /**
     * @param $file
     *
     * @return string
     */
    protected function getFile($file)
    {
        $viewDir = $this->getDir(
            'Firebear_ImportExport',
            self::MODULE_DONWLOADER_DIR
        );

        return $viewDir . "/" . $file;
    }

    /**
     * @param        $moduleName
     * @param string $type
     *
     * @return string
     */
    protected function getDir($moduleName, $type = null)
    {
        $path = $this->componentRegistrar->getPath(ComponentRegistrar::MODULE, $moduleName);

        if (!empty($type)) {
            $path .= '/' . $type;
        }

        return $path;
    }
}
