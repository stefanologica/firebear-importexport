<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */
namespace Firebear\ImportExport\Controller\Adminhtml\Job;

use Firebear\ImportExport\Controller\Adminhtml\Context;
use Firebear\ImportExport\Controller\Adminhtml\Job as JobController;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\Filesystem;
use Magento\Framework\Serialize\SerializerInterface;
use Magento\MediaStorage\Model\File\UploaderFactory;
use Magento\Store\Model\StoreManagerInterface;
use Firebear\ImportExport\Model\Import\File\Validator\NotProtectedExtension;

/**
 * Class Fileload
 *
 * @package Firebear\ImportExport\Controller\Adminhtml\Job
 */
class Fileload extends JobController
{

    const MIME_OCTET_STREAM = 'application/octet-stream';

    const MIME_CSV = 'text/csv';

    /**
     * @var Filesystem
     */
    private $fileSystem;

    /**
     * @var SerializerInterface
     */
    protected $serializer;

    /**
     * @var UploaderFactory
     */
    private $uploaderFactory;

    /**
     * @var StoreManagerInterface
     */
    private $storeManager;

    /**
     * @var NotProtectedExtension
     */
    private $validator;

    /**
     * Fileload constructor.
     *
     * @param Context $context
     * @param Filesystem $filesystem
     * @param SerializerInterface $serializer
     * @param UploaderFactory $uploaderFactory
     * @param StoreManagerInterface $storeManager
     * @param NotProtectedExtension $validator
     */
    public function __construct(
        Context $context,
        Filesystem $filesystem,
        SerializerInterface $serializer,
        UploaderFactory $uploaderFactory,
        StoreManagerInterface $storeManager,
        NotProtectedExtension $validator
    ) {
        parent::__construct($context);

        $this->fileSystem = $filesystem;
        $this->serializer = $serializer;
        $this->uploaderFactory = $uploaderFactory;
        $this->storeManager = $storeManager;
        $this->validator = $validator;
    }

    /**
     * Upload file via ajax
     */
    public function execute()
    {
        try {
            $uploader = $this->uploaderFactory->create([
                'fileId' => 'file_upload',
                'validator' => $this->validator
            ]);
            $mediaDirectory = $this->fileSystem->getDirectoryRead(DirectoryList::MEDIA);
            $root = $this->fileSystem->getDirectoryRead(DirectoryList::ROOT);
            $uploader->setAllowRenameFiles(true);
            $uploader->setFilesDispersion(true);
            if (in_array($uploader->getFileExtension(), [
                'tar',
                'gz'
            ])) {
                $uploader->skipDbProcessing(true);
                $archiveData = $uploader->save($mediaDirectory->getAbsolutePath('importexport/'));
                $phar = new \PharData($archiveData['path'] . $archiveData['file']);
                $phar->extractTo($archiveData['path'], null, true);
                $fileName = $phar->getFilename();
                $result['type'] = $phar->getExtension();
                $result['file'] = $fileName;
                $result['path'] = $archiveData['path'];
                $result['size'] = $phar->getSize();
            } elseif ($uploader->getFileExtension() == 'zip') {
                $archiveData = $uploader->save($mediaDirectory->getAbsolutePath('importexport/'));
                $file = $archiveData['path'] . $archiveData['file'];
                $zip = new \Magento\Framework\Archive\Zip();

                $zip->unpack($file, preg_replace('/\.zip$/i', '.csv', $file));
                $result['type'] = 'csv';
                $result['file'] = preg_replace('/\.zip$/i', '.csv', $archiveData['file']);
                $result['path'] = $archiveData['path'];
            } else {
                $result = $uploader->save($mediaDirectory->getAbsolutePath('importexport/'));
            }
            $result['path'] = str_replace($root->getAbsolutePath(), "", $result['path']);
            unset($result['tmp_name']);
            if ($result['type'] == self::MIME_OCTET_STREAM) {
                $result['type'] = self::MIME_CSV;
            }
            $result['url'] = $this->getTmpMediaUrl($result['file']);
            $this->getResponse()->setBody($this->serializer->serialize($result));
        } catch (\Exception $e) {
            $result = [
                'error' => $e->getMessage(),
                'errorcode' => $e->getCode()
            ];
            $this->getResponse()->setBody($this->serializer->serialize($result));
        }
    }

    /**
     * @return string
     */
    private function getBaseTmpMediaUrl()
    {
        return $this->storeManager->getStore()->getBaseUrl(\Magento\Framework\UrlInterface::URL_TYPE_MEDIA);
    }

    /**
     * @param
     *            $file
     * @return string
     */
    private function getTmpMediaUrl($file)
    {
        return $this->getBaseTmpMediaUrl() . '/' . $this->prepareFile($file);
    }

    /**
     * @param
     *            $file
     * @return string
     */
    private function prepareFile($file)
    {
        return ltrim(str_replace('\\', '/', $file), '/');
    }
}
