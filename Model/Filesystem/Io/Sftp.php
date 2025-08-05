<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Filesystem\Io;

/**
 * Extended SFTP client
 */
class Sftp extends \Magento\Framework\Filesystem\Io\Sftp
{
    const SOURCE_LOCAL_FILE = 1;

    /**
     * @param array $args
     *
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function open(array $args = [])
    {
        if (!isset($args['timeout'])) {
            $args['timeout'] = self::REMOTE_TIMEOUT;
        }
        $host = $args['host'];
        $port = ($args['port']) ? $args['port'] : self::SSH2_PORT;
        $username  = $args['username'];
        $password = $args['password'];
        $this->_connection = new \phpseclib\Net\SFTP($host, $port, $args['timeout']);
        if (!$this->_connection->login($username, $password)) {
            throw new \Magento\Framework\Exception\LocalizedException(
                __("Unable to open SFTP connection as %1@%2", $username, $password)
            );
        }
    }

    /**
     * @param      $filename
     * @param      $source
     * @param null $mode
     *
     * @return mixed
     */
    public function write($filename, $source, $mode = null)
    {
         return $this->_connection->put($filename, $source, self::SOURCE_LOCAL_FILE);
    }

    /**
     * Creates a directory.
     *
     * @param string $dir
     * @param int $mode
     * @param bool $recursive
     * @return bool
     */
    public function mkdir($dir, $mode = -1, $recursive = true)
    {
        if ($this->_connection->is_dir($dir)) {
            $result = true;
        } else {
            $result = $this->_connection->mkdir($dir, $mode = -1, $recursive = true);
        }
        return $result;
    }

    /**
     * @param $filename
     * @return mixed
     */
    public function mdtm($filename)
    {
        return $this->_connection->filemtime($filename);
    }
}
