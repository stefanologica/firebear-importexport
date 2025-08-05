<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Source\Type;

use Exception;
use Firebear\ImportExport\Model\Filesystem\File\ReadFactory;
use Firebear\ImportExport\Model\Source\Factory as SourceFactory;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\DataObject;
use Magento\Framework\Filesystem;
use Magento\Framework\Filesystem\Directory\WriteFactory as DirectoryWriteFactory;
use Magento\Framework\Filesystem\File\WriteFactory as FileWriteFactory;
use Magento\Framework\Filesystem\Directory\WriteInterface;
use Magento\Framework\Filesystem\DriverPool;
use Magento\Framework\Exception\FileSystemException;
use Magento\Framework\Exception\ValidatorException;
use Magento\Framework\Stdlib\DateTime\Timezone;
use Magento\Framework\App\CacheInterface;

/**
 * Abstract class for import source types
 *
 * @package Firebear\ImportExport\Model\Source\Type
 */
abstract class AbstractType extends DataObject
{
    /**
     * Temp directory for downloaded files
     */
    const IMPORT_DIR = 'var/import';

    /**
     * Temp directory for downloaded images
     */
    const MEDIA_IMPORT_DIR = 'pub/media/import';

    /**
     * Export directory
     */
    const EXPORT_DIR = 'var/export';

    /**
     * Files extension
     */
    const CSV_FILENAME_EXTENSION = '.csv';
    const JSON_FILENAME_EXTENSION = '.json';
    const XML_FILENAME_EXTENSION = '.xml';
    const ODS_FILENAME_EXTENSION = '.ods';
    const XLSX_FILENAME_EXTENSION = '.xlsx';

    /**
     * Source type code
     *
     * @var string
     */
    protected $code;

    /**
     * @var ScopeConfigInterface
     */
    protected $scopeConfig;

    /**
     * @var WriteInterface
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
     * @var array
     */
    protected $metadata = [];

    /**
     * @var mixed
     */
    protected $client;

    /**
     * @var mixed
     */
    protected $exportModel;

    /**
     * @var DirectoryWriteFactory
     */
    protected $writeFactory;

    /**
     * @var Timezone
     */
    protected $timezone;

    /**
     * @var FileWriteFactory
     */
    protected $fileWrite;

    /**
     * @var SourceFactory
     */
    protected $factory;

    protected $formatFile;

    /**
     * @var \Magento\Framework\App\CacheInterface
     */
    protected $cache;

    /**
     * AbstractType constructor.
     *
     * @param ScopeConfigInterface $scopeConfig
     * @param Filesystem $filesystem
     * @param ReadFactory $readFactory
     * @param DirectoryWriteFactory $writeFactory
     * @param FileWriteFactory $fileWrite
     * @param Timezone $timezone
     * @param SourceFactory $factory
     * @param array $data
     *
     * @throws FileSystemException
     */
    public function __construct(
        ScopeConfigInterface $scopeConfig,
        Filesystem $filesystem,
        ReadFactory $readFactory,
        DirectoryWriteFactory $writeFactory,
        FileWriteFactory $fileWrite,
        Timezone $timezone,
        SourceFactory $factory,
        CacheInterface $cache,
        array $data = []
    ) {
        $this->scopeConfig = $scopeConfig;
        $this->filesystem = $filesystem;
        $this->directory = $filesystem->getDirectoryWrite(DirectoryList::ROOT);
        $this->readFactory = $readFactory;
        $this->writeFactory = $writeFactory;
        $this->timezone = $timezone;
        $this->fileWrite = $fileWrite;
        $this->factory = $factory;
        $this->cache = $cache;

        parent::__construct(
            $data
        );
    }

    /**
     * Prepare temp dir for import files
     *
     * @return string
     */
    protected function getImportPath()
    {
        return self::IMPORT_DIR . '/' . $this->code;
    }

    /**
     * Prepare temp dir for import images
     *
     * @return string
     */
    protected function getMediaImportPath()
    {
        return self::MEDIA_IMPORT_DIR . '/' . $this->code;
    }

    /**
     * Get file path
     *
     * @return bool|string
     */
    public function getImportFilePath()
    {
        if ($sourceType = $this->getImportSource()) {
            $filePath = $this->getData($sourceType . '_file_path');

            return $filePath;
        }

        return false;
    }

    /**
     * Get source type code
     *
     * @return string
     */
    public function getCode()
    {
        return $this->code;
    }

    /**
     * @return mixed
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param $client
     */
    public function setClient($client)
    {
        $this->client = $client;
    }

    /**
     * @return mixed
     */
    abstract public function uploadSource();

    /**
     * @param $importImage
     * @param $imageSting
     * @return mixed
     */
    abstract public function importImage($importImage, $imageSting);

    /**
     * @param $timestamp
     * @return mixed
     */
    abstract public function checkModified($timestamp);

    /**
     * @return mixed
     */
    abstract protected function _getSourceClient();

    /**
     * @param $model
     */
    public function setExportModel($model)
    {
        $this->exportModel = $model;
    }

    /**
     * @return mixed
     */
    public function getExportModel()
    {
        return $this->exportModel;
    }

    /**
     * Return file
     *
     * @param string $path
     * @return bool
     * @throws FileSystemException
     * @throws ValidatorException
     */
    protected function writeFile($path)
    {
        $result = false;
        $directory = null;
        $fileContent = $this->getExportModel()->export();
        if ($fileContent) {
            $newPath = $this->clearPath($path);
            $dir = $this->filesystem->getDirectoryRead(DirectoryList::ROOT);
            if (count($newPath) > 1) {
                $fileName = array_pop($newPath);
                $path = implode("/", $newPath);
                if (!$dir->isExist($path)) {
                    $directory = $this->writeFactory->create($dir->getAbsolutePath($path), DriverPool::FILE, 0775);
                    $directory->create();
                }
                $path = $dir->getAbsolutePath() . $path . "/";
            } else {
                $fileName = array_pop($newPath);
                $path = $dir->getAbsolutePath();
            }

            $page = $this->cache->load('current_page');

            if (($page > 1) && ($dir->isExist($path . $fileName))) {
                $read = $this->readFactory->create($path . $fileName, DriverPool::FILE);
                $fileContent = $read->readAll() . $fileContent;
            }

            $file = $this->fileWrite->create(
                $path . $fileName,
                DriverPool::FILE,
                "w"
            );
            $file->write($fileContent);
            $file->close();

            $stat = $file->stat($path . $fileName);
            $result = !empty($stat['size']);
        }
        return $result;
    }

    /**
     * @param string $path
     *
     * @return array
     */
    protected function clearPath($path)
    {
        $arrayPath = explode("/", $path);
        $newArrayPath = [];
        foreach ($arrayPath as $partPath) {
            if (!empty($partPath)) {
                $newArrayPath[] = $partPath;
            }
        }

        return $newArrayPath;
    }

    /**
     * @return bool
     */
    public function check()
    {
        try {
            if ($client = $this->_getSourceClient()) {
                return true;
            }
        } catch (Exception $e) {
            return false;
        }

        return false;
    }

    /**
     * @param string $importImage
     * @param string $imageSting
     * @param array $matches
     * @throws FileSystemException
     */
    public function setUrl($importImage, $imageSting, $matches)
    {
        $url = str_replace($matches[0], '', $importImage);
        $read = $this->readFactory->create($url, DriverPool::HTTP);
        $this->directory->writeFile(
            $this->directory->getAbsolutePath($this->getMediaImportPath() . $imageSting),
            $read->readAll()
        );
    }

    /**
     * @param string $file
     * @return $this
     */
    public function setFormatFile($file)
    {
        $this->formatFile = $file;

        return $this;
    }

    /**
     * @return string
     */
    public function getFormatFile()
    {
        return $this->formatFile;
    }

    /**
     * @param string $url
     *
     * @return string
     */
    public function convertUrlToFilename($url)
    {
        $parsedUrl = parse_url($url);
        $filename = str_replace('.', '_', $parsedUrl['host'])
            . str_replace('/', '_', $parsedUrl['path'])
            . constant('self::' . strtoupper($this->getData('type_file')) . '_FILENAME_EXTENSION');

        return $filename;
    }
}
