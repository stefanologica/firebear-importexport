<?php
/**
 * @copyright: Copyright © 2019 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */
namespace Firebear\ImportExport\Model\ResourceModel;

use Magento\Framework\Encryption\EncryptorInterface;
use Magento\Framework\DataObject;

/**
 * Field Encryptor
 */
class FieldEncryptor
{
    /**
     * Encryptor
     *
     * @var EncryptorInterface
     */
    protected $encryptor;

    /**
     * Fields that should be encrypted
     *
     * @var array
     */
    protected $encryptableFields = [
        'export_source_ftp_password',
        'export_source_sftp_password',
        'password'
    ];

    /**
     * @param EncryptorInterface $encryptor
     */
    public function __construct(
        EncryptorInterface $encryptor
    ) {
        $this->encryptor = $encryptor;
    }

    /**
     * Encrypt a string
     *
     * @param mixed[] $data
     * @return mixed[]
     */
    public function encrypt(array $data)
    {
        foreach ($this->encryptableFields as $field) {
            $value = $data[$field] ?? null;
            if (!empty($value)) {
                if (true !== $this->encryptor->validateHashVersion($value)) {
                    continue;
                }
                $encrypted = $this->encryptor->encrypt($value);
                $data[$field] = $encrypted;
            }
        }
        return $data;
    }

    /**
     * Decrypt a string
     *
     * @param mixed[] $data
     * @return mixed[]
     */
    public function decrypt(array $data)
    {
        foreach ($this->encryptableFields as $field) {
            $value = $data[$field] ?? null;
            if (!empty($value)) {
                if (true === $this->encryptor->validateHashVersion($value)) {
                    continue;
                }
                $decrypted = $this->encryptor->decrypt($value);
                if ($decrypted) {
                    $data[$field] = $decrypted;
                }
            }
        }
        return $data;
    }
}
