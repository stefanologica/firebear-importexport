<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */
namespace Firebear\ImportExport\Model\Export\Dependencies;

interface ConfigInterface
{
    /**
     * Get configuration of entity by name
     *
     * @param string $name
     * @return array
     */
    public function getEntity($name);
}
