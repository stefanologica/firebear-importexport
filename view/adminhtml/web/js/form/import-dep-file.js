/**
 * @copyright: Copyright © 2019 Firebear Studio. All rights reserved.
 * @author   : Firebear Studio <fbeardev@gmail.com>
 */
define(
    [
        'underscore',
        'Magento_Ui/js/form/element/abstract',
        'uiRegistry'
    ],
    function (_, Element, reg) {
        'use strict';

        return Element.extend(
            {
                defaults: {
                    valuesForOptions: [],
                    imports: {
                        toggleBySource: '${$.parentName}.import_source:value',
                        toggleByPlatform: 'import_job_form.import_job_form.settings.platforms:value'
                    },
                    isShown: false,
                    inverseVisibility: false,
                    visible:false
                },

                toggleVisibility: function (selected) {
                    this.isShown = selected in this.valuesForOptions;
                    this.visible(this.inverseVisibility ? !this.isShown : this.isShown);
                },

                toggleBySource: function (selected) {
                    if (!selected || selected === undefined) {
                        return;
                    }
                    this.toggleVisibility(selected);
                },

                toggleByPlatform: function (selected) {
                    var defaultSource = reg.get(this.parentName + '.import_source').getInitialValue();
                    if (!defaultSource) {
                        defaultSource = 'file';
                    }
                    if (!selected || selected === undefined) {
                        this.toggleVisibility(defaultSource);
                        return;
                    }

                    var entity = reg.get(this.ns + '.' + this.ns + '.settings.entity');
                    if (entity === undefined) {
                        this.toggleVisibility(selected);
                        return;
                    }
                    var value = entity.value() + '_' + selected;
                    value = value in this.platformForm ? value : defaultSource;
                    this.toggleVisibility(value);
                }
            }
        );
    }
);
