<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\ResourceModel\Job;

use Magento\Framework\Model\ResourceModel\Db\VersionControl\RelationInterface;
use Magento\Framework\Exception\AlreadyExistsException;

/**
 * Class Relation
 */
class Relation implements RelationInterface
{
    /**
     * Code of "Integrity constraint violation: 1062 Duplicate entry" error
     */
    const ERROR_CODE_DUPLICATE_ENTRY = 23000;

    /**
     * @var \Firebear\ImportExport\Api\JobMappingRepositoryInterface
     */
    private $mappingRepository;

    /**
     * Relation constructor.
     *
     * @param \Firebear\ImportExport\Api\JobMappingRepositoryInterface $mappingRepository
     */
    public function __construct(
        \Firebear\ImportExport\Api\JobMappingRepositoryInterface $mappingRepository
    ) {
        $this->mappingRepository = $mappingRepository;
    }

    /**
     * @param \Magento\Framework\Model\AbstractModel $object
     *
     * @throws AlreadyExistsException
     * @throws \Exception
     */
    public function processRelation(\Magento\Framework\Model\AbstractModel $object)
    {
        if (null !== $object->getMap()) {
            foreach ($object->getMap() as $map) {
                if (!$map->getJobId()) {
                    $map->setJobId($object->getId());
                }
                try {
                    $this->mappingRepository->save($map);
                } catch (\Exception $e) {
                    if ($e->getCode() === self::ERROR_CODE_DUPLICATE_ENTRY
                        && preg_match('#SQLSTATE\[23000\]: [^:]+: 1062[^\d]#', $e->getMessage())
                    ) {
                        throw new AlreadyExistsException(
                            __('Duplicated attributes.')
                        );
                    }
                    throw $e;
                }
            }
        }
    }
}
