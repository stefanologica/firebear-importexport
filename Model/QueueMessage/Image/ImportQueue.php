<?php

namespace Firebear\ImportExport\Model\QueueMessage\Image;

use Firebear\ImportExport\Model\Import\Product\ImageProcessor;

/**
 * Class ImportQueue
 * @package Firebear\ImportExport\Model\QueueMessage\Image
 */
class ImportQueue
{
    /**
     * @var ImageProcessor
     */
    protected $imageProcessor;

    /**
     * ImportQueue constructor.
     * @param ImageProcessor $imageProcessor
     */
    public function __construct(ImageProcessor $imageProcessor)
    {
        $this->imageProcessor = $imageProcessor;
    }

    /**
     * @param $message
     */
    public function process($message)
    {
        $this->imageProcessor->processImportImages($message);
    }
}
