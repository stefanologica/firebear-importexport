<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Filesystem\Io;

/**
 * Extended FTP client
 */
class Ftp extends \Magento\Framework\Filesystem\Io\Ftp
{
    /**
     * Returns the last modified time of the given file
     * Note: Not all servers support this feature! Does not work with directories.
     *
     * @param string $filename
     *
     * @see http://php.net/manual/en/function.ftp-mdtm.php
     *
     * @return int
     */
    public function mdtm($filename)
    {
        return ftp_mdtm($this->_conn, $filename);
    }

    /**
     * Creates a directory.
     *
     * @param string $dir
     * @param int $mode
     * @param bool $recursive
     * @return bool|void
     */
    public function mkdir($dir, $mode = 0777, $recursive = false)
    {
        if (!$this->cd($dir)) {
            if ($recursive) {
                $parts = explode(DIRECTORY_SEPARATOR, $dir);
                foreach ($parts as $part) {
                    if (!$this->cd($part)) {
                        parent::mkdir($part);
                        $this->cd($part);
                        $this->chmod($mode, $part);
                    }
                }
            } else {
                parent::mkdir($dir);
                $this->chmod($mode, $dir);
            }
        } else {
            $this->chmod($mode, $dir);
        }
    }

    public function checkIsPath($filename, $dest)
    {
        try {
            $result = ftp_get($this->_conn, $dest, $filename, $this->_config['file_mode']);
        } catch (\Exception $e) {
            $result = false;
        }

        return $result;
    }
}
