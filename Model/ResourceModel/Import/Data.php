<?php
/**
 * @copyright: Copyright © 2019 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\ResourceModel\Import;

use Magento\Framework\Exception\ValidatorException;

/**
 * Class Data
 *
 * @package Firebear\ImportExport\Model\ResourceModel\Import
 */
class Data extends \Magento\ImportExport\Model\ResourceModel\Import\Data
{
    /**
     * @var null
     */
    protected $job = null;

    /**
     * @var null
     */
    protected $file = null;

    protected $offset = 0;

    protected function _construct()
    {
        $this->_init('firebear_importexport_importdata', 'id');
    }

    /**
     * Return behavior from import data table.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->getUniqueColumnData('job_id');
    }

    /**
     * Retrieve an external iterator
     *
     * @return \Iterator
     */
    public function getIterator()
    {
        $connection = $this->getConnection();
        $select = $connection->select()->from($this->getMainTable(), ['data'])
            ->order('id ASC')
            ->limit(1, $this->getOffset());
        if ($this->getJob()) {
            $select->where('job_id=?', $this->getJobId());
        }
        if ($this->getFile()) {
            $select->where('file=?', $this->getFile());
        }
        $stmt = $connection->query($select);
        $stmt->setFetchMode(\Zend_Db::FETCH_NUM);
        if ($stmt instanceof \IteratorAggregate) {
            $iterator = $stmt->getIterator();
        } else {
            // Statement doesn't support iterating, so fetch all records and create iterator ourself
            $rows = $stmt->fetchAll();
            $iterator = new \ArrayIterator($rows);
        }

        return $iterator;
    }

    /**
     * Clean all bunches from table.
     *
     * @return int
     * @throws \Exception
     */
    public function cleanBunches()
    {
        return $this->getConnection()->delete($this->getMainTable());
    }

    /**
     * @param $entity
     * @param $behavior
     * @param null $jobId
     * @param null $file
     * @param array $data
     * @return int
     */
    public function saveBunches(
        $entity,
        $behavior,
        $jobId = null,
        $file = null,
        array $data = []
    ) {
        $encodedData = $this->jsonHelper->jsonEncode($data);
        if (json_last_error() !== JSON_ERROR_NONE && empty($encodedData)) {
            throw new ValidatorException(
                __('Error in CSV: ' . json_last_error_msg())
            );
        }

        return $this->getConnection()->insert(
            $this->getMainTable(),
            [
                'behavior' => $behavior,
                'entity' => $entity,
                'job_id' => $jobId,
                'file' => $file,
                'data' => $encodedData
            ]
        );
    }

    /**
     * @return null
     */
    public function getFile()
    {
        return $this->file;
    }

    /**
     * @param $file
     * @return $this
     */
    public function setFile($file)
    {
        $this->file = $file;

        return $this;
    }

    /**
     * @return null
     */
    public function getJob()
    {
        return $this->job;
    }

    /**
     * @param $job
     * @return $this
     */
    public function setJob($job)
    {
        $this->job = $job;

        return $this;
    }

    /**
     * @return null
     */
    public function getOffset()
    {
        return $this->offset;
    }

    /**
     * @param $offset
     * @return $this
     */
    public function setOffset($offset)
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @return array
     */
    public function getCounts($job, $file)
    {
        $connection = $this->getConnection();
        $select = $connection->select()->from($this->getMainTable(), ['count(id)'])->order('id ASC');
        $select->where('job_id=?', $job);
        $select->where('file=?', $file);
        $stmt = $connection->query($select);
        $stmt->setFetchMode(\Zend_Db::FETCH_NUM);

        return $stmt->fetch();
    }

    /**
     * @return integer
     */
    public function getCount($job, $file)
    {
        $select = $this->getConnection()->select()
            ->from($this->getMainTable(), ['COUNT(*)'])
            ->where('job_id=?', $job)
            ->where('file=?', $file);

        return $this->getConnection()->fetchOne($select);
    }
}
