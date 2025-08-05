<?php
/**
 * @copyright: Copyright © 2019 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Api;

interface JobManagementInterface
{
    /**
     * Upload file to import directory.
     *
     * @param string|null $fileName
     * @param int|bool    $uniqueName
     *
     * @return string
     */
    public function fileUpload($fileName = null, $uniqueName = null);
}
