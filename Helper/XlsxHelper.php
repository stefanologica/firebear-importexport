<?php
/**
 * XlsxHelper
 *
 * @copyright Copyright © 2019 Firebear Studio. All rights reserved.
 * @author    fbeardev@gmail.com
 */

namespace Firebear\ImportExport\Helper;

use Box\Spout\Common\Exception\UnsupportedTypeException;
use Box\Spout\Common\Type;
use Box\Spout\Reader\ReaderFactory;
use Box\Spout\Reader\XLSX\Reader;
use Box\Spout\Reader\XLSX\Sheet;
use Exception;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Framework\Filesystem\Directory\ReadInterface;
use Magento\Framework\FilesystemFactory;
use function class_exists;

class XlsxHelper extends AbstractHelper
{
    /**
     * @var ReadInterface
     */
    protected $directory;
    /**
     * @var Reader
     */
    protected $reader;
    /**
     * @var Spout
     */
    private $spout;

    /**
     * XlsxHelper constructor.
     * @param Context $context
     * @param FilesystemFactory $filesystemFactory
     * @param Spout $spout
     * @throws UnsupportedTypeException
     */
    public function __construct(
        Context $context,
        FilesystemFactory $filesystemFactory,
        Spout $spout
    ) {
        parent::__construct($context);
        $this->directory = $filesystemFactory->create()->getDirectoryRead(DirectoryList::ROOT);
        if ($spout->isSpoutInstall()) {
            $this->reader = ReaderFactory::create(Type::XLSX);
        }
        $this->spout = $spout;
    }

    /**
     * @param string $file
     * @return array
     */
    public function getSheetsOptions(string $file)
    {
        $sheetsName = [];
        try {
            if ($this->spout->isSpoutInstall() && ($this->reader instanceof Reader)) {
                $file = $this->directory->getAbsolutePath($file);
                $this->reader->setShouldFormatDates(true);
                $this->reader->open($file);
                /** @var Sheet $sheet */
                foreach ($this->reader->getSheetIterator() as $sheet) {
                    $sheetsName[] = ['label' => $sheet->getName(), 'value' => $sheet->getIndex() + 1];
                }
                $this->reader->close();
            }
        } catch (Exception $exception) {
            $this->_logger->critical($exception->getMessage());
        }
        return $sheetsName;
    }
}
