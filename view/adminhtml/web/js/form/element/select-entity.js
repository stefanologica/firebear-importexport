    /**
 * @copyright: Copyright © 2017 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */

define(
    [
    'jquery',
    'underscore',
    'Magento_Ui/js/form/element/select',
    'Firebear_ImportExport/js/form/element/general',
    'uiRegistry'
    ],
    function ($, _, Acstract, general, reg) {
        'use strict';
        return Acstract.extend(general).extend(
            {
                defaults: {
                    sourceExt: null,
                    sourceOptions: null,
                    imports: {
                        changeSource: '${$.ns}.${$.ns}.settings.entity:value',
                        addDependency: '${$.ns}.${$.ns}.behavior.behavior_field_order:value'
                    },
                    listens: {
                        '${$.ns}.${$.ns}.behavior.behavior_field_order:value':'addDependency'
                    }
                },
                initialize: function () {
                    this._super();
                    return this;
                },
                initConfig: function (config) {
                    this._super();
                    this.sourceOptions = $.parseJSON(this.sourceOptions);
                    return this;
                },
                configureDataScope: function () {
                    var recordId,
                        prefixName,
                        suffixName;

                    // Get recordId
                    recordId = this.parentName.split('.').last();

                    prefixName = this.dataScopeToHtmlArray(this.prefixName);
                    this.elementName = this.prefixElementName + recordId;

                    suffixName = '';

                    if (!_.isEmpty(this.suffixName) || _.isNumber(this.suffixName)) {
                        suffixName = '[' + this.suffixName + ']';
                    }
                    this.inputName = prefixName + '[' + this.elementName + ']' + suffixName;

                    suffixName = '';

                    if (!_.isEmpty(this.suffixName) || _.isNumber(this.suffixName)) {
                        suffixName = '.' + this.suffixName;
                    }
                    this.exportDataLink = 'data.' + this.prefixName + '.' + this.elementName + suffixName;
                    this.exports.value = this.provider + ':' + this.exportDataLink;
                },
                changeSource: function (value) {
                    this.sourceExt = value;
                    if (value in this.sourceOptions) {
                        var newData = [];
                        var data = this.sourceOptions[value];
                        _.each(data, function (index) {
                            if (!('labeltitle' in index)) {
                                index.labeltitle = index.label;
                            }
                            newData.push(index);
                        });
                        this.setOptions(newData).addDependency();
                    }
                },
                addDependency: function (dep) {
                    var entity = reg.get(this.ns + '.' + this.ns + '.settings.entity');
                    var field = reg.get(this.ns + '.' + this.ns + '.behavior.behavior_field_' + entity.value());

                    if (field !== undefined) {
                        dep = field.value();
                        if (_.size(dep) > 0) {
                            var valueEl = this.value();
                            var select = reg.get(this.parentName + '.source_data_system');
                            if (select != undefined) {
                                var oldValue = select.value();
                            }
                            var options = this.sourceOptions[this.sourceExt];
                            var newOptions = [];
                            _.each(
                                options,
                                function (value, key) {
                                    if (value.dep != undefined) {
                                        if (_.indexOf(dep, value.value) != -1) {
                                            newOptions.push(value);
                                        }
                                    }
                                }
                            );
                            this.setOptions(newOptions);
                            this.value(valueEl);
                            if (select != undefined) {
                                select.value(oldValue);
                            }
                        } else {
                            var newOptions = [];
                            this.setOptions(newOptions);
                        }
                    }
                }
            }
        )
    }
);
