<?php
/**
 * @copyright: Copyright © 2018 Firebear Studio GmbH. All rights reserved.
 * @author: Firebear Studio <fbeardev@gmail.com>
 */

namespace Firebear\ImportExport\Model\Import\Product\Price\Rule;

/**
 * Class Condition
 *
 * @package Firebear\ImportExport\Model\Import\Product\Price\Rule
 */
class Condition extends \Magento\Rule\Model\Condition\AbstractCondition
{
    /**
     * @var \Magento\Catalog\Model\Product\Attribute\Repository
     */
    private $attributeRepository;

    /**
     * Condition constructor.
     * @param \Magento\Rule\Model\Condition\Context $context
     * @param \Magento\Catalog\Model\Product\Attribute\Repository $attributeRepository
     * @param array $data
     */
    public function __construct(
        \Magento\Rule\Model\Condition\Context $context,
        \Magento\Catalog\Model\Product\Attribute\Repository $attributeRepository,
        array $data = []
    ) {
        parent::__construct($context, $data);
        $this->attributeRepository = $attributeRepository;
    }

    /**
     * @param $data
     * @return bool
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    public function validatePriceRuleConditions($data)
    {
        $conditions = $data['conditions'];
        $rowData = $data['row'];
        $aggr = $data['aggregator'];
        $aggrValue = $data['value'];
        $categoryIds = $data['categories'];

        $applyRule = ($aggr == 'any') ? false : true;

        foreach ($conditions as $key => $condition) {
            if (strpos($key, '--') !== false) {
                $attribute = $condition['attribute'];
                $operator = $condition['operator'];
                $value = $condition['value'];
                $this->setData('value_parsed', $value);
                $this->setOperator($operator);

                switch ($attribute) {
                    case 'category_ids':
                        $value = array_map('intval', explode(',', $value));
                        if (count($value) > 1) {
                            $this->setData('value_parsed', $value);
                        }

                        $validationResult = $this->validateAttribute($categoryIds);

                        break;
                    default:
                        if (isset($rowData[$attribute])) {
                            $rowValue = $rowData[$attribute];

                            if (!is_array($value)) {
                                $attributeLabel = $this->getProductAttributeLabelByValue($attribute, $value);
                                if ($attributeLabel) {
                                    $value = $attributeLabel;
                                    $this->setData('value_parsed', $value);
                                }
                                $validationResult = $this->validateAttribute($rowValue);
                            } else {
                                $this->_inputType = 'multiselect';
                                foreach ($value as $key => $attributeValue) {
                                    $attributeLabel = $this->getProductAttributeLabelByValue(
                                        $attribute,
                                        $attributeValue
                                    );
                                    if ($attributeLabel) {
                                        $value[$key] = $attributeLabel;
                                    }
                                }
                                if (!is_array($rowValue)) {
                                    $rowValue = [$rowValue];
                                }
                                $this->setData('value_parsed', $value);
                                $validationResult = $this->validateAttribute($rowValue);
                                $this->_inputType = null;
                            }
                        } else {
                            $validationResult = false;
                        }

                        break;
                }

                if ($aggr == 'any') {
                    $applyRule = $aggrValue ? ($applyRule || $validationResult) : ($applyRule || !$validationResult);
                } else {
                    $applyRule = $aggrValue ? ($applyRule && $validationResult) : ($applyRule && !$validationResult);
                }
            }
        }

        return $applyRule;
    }

    /**
     * @param $attribute
     * @param $value
     * @return null|string
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    private function getProductAttributeLabelByValue($attribute, $value)
    {
        $label = null;
        $attribute = $this->attributeRepository->get($attribute);
        if ($attribute) {
            $attributeOptions = $attribute->getOptions();
            if (!empty($attributeOptions)) {
                foreach ($attribute->getOptions() as $option) {
                    if ($value == $option->getValue()) {
                        $label = $option->getLabel();
                    }
                }
            }
        }
        return $label;
    }
}
