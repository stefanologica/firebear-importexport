<?php
/**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Source\Config;

use Magento\Framework\Option\ArrayInterface;

/**
 * Class CartPrice
 *
 * @package Firebear\ImportExport\Model\Source\Config
 */
class CartPrice implements ArrayInterface
{
    /**
     * Convert dom node tree to array
     *
     * @param \DOMDocument $source
     * @return array
     * @throws \InvalidArgumentException
     *
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     */
    public function toOptionArray()
    {
        return [
        ['value'=>'name','label'=>__('Name')],
        ['value'=>'is_active','label'=>__('Is Active')],
        ['value'=>'website_ids','label'=>__('Website Ids')],
        ['value'=>'customer_group_ids','label'=>__('Customer Group Ids')],
        ['value'=>'coupon_type','label'=>__('Coupon Type')],
        ['value'=>'coupon_code','label'=>__('Coupon Code')],
        ['value'=>'discount_amount','label'=>__('Discount Amount')]
        ];
    }
    public function toArray()
    {
        return [
            __('name'),
            __('is_active'),
            __('website_ids'),
            __('customer_group_ids'),
            __('coupon_type'),
            __('coupon_code'),
            __('discount_amount')
        ];
    }
}
