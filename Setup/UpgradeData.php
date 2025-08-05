<?php
/**
 * UpgradeData
 *
 * @copyright Copyright © 2018 Firebear Studio. All rights reserved.
 * @author    Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Setup;

use Firebear\ImportExport\Setup\Operations\CreateCmsEntityTypes;
use Firebear\ImportExport\Setup\Operations\EncryptPasswordFields;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\ModuleDataSetupInterface;
use Magento\Framework\Setup\UpgradeDataInterface;

/**
 * Upgrade Data script
 *
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 */
class UpgradeData implements UpgradeDataInterface
{
    /**
     * @var CreateCmsEntityTypes
     */
    protected $createCmsEntityTypes;

    /**
     * @var EncryptPasswordFields
     */
    protected $encryptPasswordFields;

    /**
     * UpgradeData constructor.
     *
     * @param CreateCmsEntityTypes $createCmsEntityTypes
     * @param EncryptPasswordFields $encryptPasswordFields
     */
    public function __construct(
        CreateCmsEntityTypes $createCmsEntityTypes,
        EncryptPasswordFields $encryptPasswordFields
    ) {
        $this->createCmsEntityTypes = $createCmsEntityTypes;
        $this->encryptPasswordFields = $encryptPasswordFields;
    }

    /**
     * Upgrades data for a module
     *
     * @param ModuleDataSetupInterface $setup
     * @param ModuleContextInterface $context
     *
     * @return void
     */
    public function upgrade(ModuleDataSetupInterface $setup, ModuleContextInterface $context)
    {
        $setup->startSetup();
        if (version_compare($context->getVersion(), '2.1.2', '<')) {
            $this->createCmsEntityTypes->execute($setup);
        }
        if (version_compare($context->getVersion(), '2.1.4', '<')) {
            $this->createCmsEntityTypes->execute($setup);
        }
        if (version_compare($context->getVersion(), '3.5.0-alpha.4', '<')) {
            $this->encryptPasswordFields->execute($setup);
        }

        $setup->endSetup();
    }
}
