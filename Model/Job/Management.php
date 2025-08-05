<?php
/**
 * @copyright: Copyright © 2019 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Job;

/**
 * Class Management
 *
 * @package Firebear\ImportExport\Model\Job
 */
class Management implements \Firebear\ImportExport\Api\JobManagementInterface
{
    /**
     * Target directory
     */
    const UPLOAD_DIRECTORY = 'importexport';

    /**
     * @var array
     */
    public static $allowedExtensions = ['csv', 'xml', 'xls', 'xlsx', 'zip'];

    /**
     * @var \Magento\Framework\Filesystem
     */
    protected $filesystem;

    /**
     * @var \Magento\MediaStorage\Model\File\UploaderFactory
     */
    protected $uploaderFactory;

    /**
     * Management constructor.
     *
     * @param \Magento\Framework\Filesystem                    $filesystem
     * @param \Magento\MediaStorage\Model\File\UploaderFactory $uploaderFactory
     */
    public function __construct(
        \Magento\Framework\Filesystem $filesystem,
        \Magento\MediaStorage\Model\File\UploaderFactory $uploaderFactory
    ) {
        $this->filesystem = $filesystem;
        $this->uploaderFactory = $uploaderFactory;
    }

    /**
     * {@inheritdoc}
     */
    public function fileUpload($fileName = '', $uniqueName = false)
    {
        $uploader = $this->uploaderFactory->create([
            'fileId' => 'file'
        ]);
        $mediaDirectory = $this->filesystem->getDirectoryRead(\Magento\Framework\App\Filesystem\DirectoryList::MEDIA);
        $uploader->setAllowRenameFiles(true);
        $uploader->setFilesDispersion(true);
        $fileExt = strtolower($uploader->getFileExtension());

        if (in_array($fileExt, self::$allowedExtensions)) {
            if ($uniqueName) {
                $fileName = date('Y-m-d-H-m-s');
            }

            $fileName = $this->correctFileName($fileName, $fileExt);

            $result = $uploader->save($mediaDirectory->getAbsolutePath(self::UPLOAD_DIRECTORY), $fileName);

            $extension = pathinfo($result['file'], PATHINFO_EXTENSION);
            $file = $result['path'] . $result['file'];

            if ($extension == 'zip') {
                $zipFile = $file;
                $zip = new \ZipArchive();
                $zip->open($zipFile);
                $file = $zip->getNameIndex(0);
                $zip->extractTo(dirname($zipFile), $file);
                $zip->close();
                unlink($zipFile);
            }

            return $mediaDirectory->getRelativePath($file);
        } else {
            throw new \Magento\Framework\Exception\LocalizedException(__('Unsupported file type'));
        }
    }

    /**
     * Add extension to file name if not exist.
     *
     * @param $name
     * @param $extension
     *
     * @return string
     */
    protected function correctFileName($name, $extension)
    {
        if ($name) {
            $newFileNameInfo = pathinfo($name);

            if (!isset($newFileNameInfo['extension'])
                || $newFileNameInfo['extension'] != $extension
            ) {
                $name .= '.' . $extension;
            }
        }

        return $name;
    }
}
