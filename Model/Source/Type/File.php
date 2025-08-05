<?php

namespace Firebear\ImportExport\Model\Source\Type;

/**
 * Class File
 *
 * @package Firebear\ImportExport\Model\Source\Type
 */
class File extends AbstractType
{
    /**
     * @var string
     */
    protected $code = 'file';

    /**
     * @return null
     */
    public function uploadSource()
    {
        return null;
    }

    /**
     * @param $importImage
     * @param $imageSting
     *
     * @return null
     */
    public function importImage($importImage, $imageSting)
    {
        return null;
    }

    /**
     * @param $timestamp
     *
     * @return null
     */
    public function checkModified($timestamp)
    {
        return null;
    }

    /**
     * @return null
     */
    protected function _getSourceClient()
    {
        return null;
    }

    /**
     * @param $model
     * @return array
     */
    public function run($model)
    {
        $result = true;
        $errors = [];
        $file = '';
        try {
            $this->setExportModel($model);
            $data = $model->getData(\Firebear\ImportExport\Model\ExportJob\Processor::EXPORT_SOURCE);
            $currentDate = "";
            if ($data['date_format']) {
                $format = $data['date_format'] ?? 'Y-m-d-hi';
                $currentDate = "-" . $this->timezone->date()->format($format);
            }
            $info = pathinfo($data['file_path']);
            $file =  $info['dirname'] . '/' . $info['filename'] . $currentDate;
            if (isset($info['extension'])) {
                $file .=  '.' . $info['extension'];
            }
            $file = $this->prepareFilePath($file);
            $result = $this->writeFile($file);
        } catch (\Exception $e) {
            $errors[] = $e->getMessage();
            if (empty($errors)) {
                $errors[] = __('No products with tier prices in catalog found.');
            }
            $result = false;
        }

        return [$result, $file, $errors];
    }

    /**
     * @param $path
     * @return mixed|null|string|string[]
     */
    public function prepareFilePath($path)
    {
        $path = str_replace(['\\'], DIRECTORY_SEPARATOR, $path);
        $path = preg_replace('|([/]+)|s', DIRECTORY_SEPARATOR, $path);
        $path = rtrim($path, DIRECTORY_SEPARATOR);
        return $path;
    }
}
