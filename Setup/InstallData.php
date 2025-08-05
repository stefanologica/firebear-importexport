<?php
declare(strict_types=1);

namespace Firebear\ImportExport\Setup;

use Firebear\ImportExport\Setup\Operations\CreateCmsEntityTypes;
use Magento\Framework\Setup\InstallDataInterface;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\ModuleDataSetupInterface;

/**
 * Class InstallData
 *
 * @package Firebear\ImportExport\Setup
 */
class InstallData implements InstallDataInterface
{
    /**
     * @var CreateCmsEntityTypes
     */
    private $createCmsEntityTypes;

    /**
     * InstallData constructor
     *
     * @param CreateCmsEntityTypes $createCmsEntityTypes
     */
    public function __construct(
        CreateCmsEntityTypes $createCmsEntityTypes
    ) {
        $this->createCmsEntityTypes = $createCmsEntityTypes;
    }

    /**
     * Installs data for a module
     *
     * @param ModuleDataSetupInterface $setup
     * @param ModuleContextInterface $context
     * @return void
     */
    public function install(
        ModuleDataSetupInterface $setup,
        ModuleContextInterface $context
    ) {
        $this->createCmsEntityTypes->execute(
            $setup
        );
    }
}
