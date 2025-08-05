<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Export\Adapter;

use Box\Spout\Common\Exception\IOException;
use Box\Spout\Common\Exception\UnsupportedTypeException;
use Box\Spout\Common\Type;
use Box\Spout\Writer\Common\Helper\CellHelper;
use Box\Spout\Writer\WriterFactory;
use Firebear\ImportExport\Model\Export\Adapter\Spout\CellHelper as FirebearCellHelper;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Exception\ValidatorException;
use Magento\Framework\Filesystem;
use Psr\Log\LoggerInterface;

/**
 * Ods Export Adapter
 */
class Ods extends AbstractAdapter
{

    /**
     * Spreadsheet Writer
     *
     * @var
     */
    protected $writer;

    /**
     * File Path
     *
     * @var string|bool
     */
    protected $filePath;

    /**
     * Ods constructor.
     *
     * @param Filesystem $filesystem
     * @param LoggerInterface $logger
     * @param null $destination
     * @param string $destinationDirectoryCode
     * @param array $data
     * @throws LocalizedException
     */
    public function __construct(
        Filesystem $filesystem,
        LoggerInterface $logger,
        $destination = null,
        $destinationDirectoryCode = DirectoryList::VAR_DIR,
        array $data = []
    ) {
        if (empty($data['export_source']['file_path'])) {
            throw new LocalizedException(__('Export File Path is Empty.'));
        }

        class_alias(FirebearCellHelper::class, CellHelper::class);

        parent::__construct($filesystem, $logger, $destination, $destinationDirectoryCode, $data);
    }

    /**
     * Write row data to source file
     *
     * @param array $rowData
     * @return AbstractAdapter
     * @throws LocalizedException
     */
    public function writeRow(array $rowData)
    {
        $rowData = $this->_prepareRow($rowData);
        if (null === $this->_headerCols) {
            $this->setHeaderCols(array_keys($rowData));
        }
        $rowData = array_merge(
            $this->_headerCols,
            array_intersect_key($rowData, $this->_headerCols)
        );
        $this->writer->addRow($rowData);
        return $this;
    }

    /**
     * Prepare Row Data
     *
     * @param array $rowData
     * @return array $rowData
     */
    protected function _prepareRow(array $rowData)
    {
        foreach ($rowData as $key => $value) {
            $rowData[$key] = (string)$value;
        }
        return $rowData;
    }

    /**
     * Set column names
     *
     * @param array $headerColumns
     * @return AbstractAdapter
     * @throws LocalizedException
     */
    public function setHeaderCols(array $headerColumns)
    {
        if (null !== $this->_headerCols) {
            throw new LocalizedException(__('The header column names are already set.'));
        }
        if ($headerColumns) {
            foreach ($headerColumns as $columnName) {
                $this->_headerCols[$columnName] = false;
            }
            $this->writer->addRow(array_keys($this->_headerCols));
        }
        return $this;
    }

    /**
     * Get contents of export file
     *
     * @return string
     */
    public function getContents()
    {
        $this->writer->close();
        return parent::getContents();
    }

    /**
     * MIME-type for 'Content-Type' header
     *
     * @return string
     */
    public function getContentType()
    {
        return 'application/vnd.oasis.opendocument.spreadsheet';
    }

    /**
     * Return file extension for downloading
     *
     * @return string
     */
    public function getFileExtension()
    {
        return 'ods';
    }

    /**
     * Method called as last step of object instance creation
     *
     * @return AbstractAdapter
     * @throws IOException
     * @throws UnsupportedTypeException
     * @throws ValidatorException
     */
    protected function _init()
    {
        $this->writer = WriterFactory::create(Type::ODS);
        $file = $this->_directoryHandle->getAbsolutePath(
            $this->_destination
        );
        $this->writer->openToFile($file);
        return $this;
    }
}
