import Ember from 'ember';

export default Ember.Component.extend({
    tagName: '',
    extractedItems: Ember.inject.service(),

    type: Ember.computed.readOnly('extractedItems.type'),
    changes: Ember.computed('extractedItems.changes', function() {
        let changes = this.get('extractedItems.changes');
        return changes ? changes : [];
    }),

    hasChanges: Ember.computed('changes', function() {
        return this.get('changes').length > 0;
    }),

    hasWarning: Ember.computed('type', function() {
        if ((this.get('type') === 'js' && this.get('changes').length < 1) ||
                this.get('hasChanges')) {
            return true;
        }
        return false;
    }),

    icon: Ember.computed('hasWarning', function() {
        return this.get('hasWarning') ? 'warning-triangle' : 'ok';
    }),

    changesText: Ember.computed('changes.[]', function() {
        if (!this.get('hasWarning')) {
            return 'Your sample is correctly configured for extraction';
        }
        if (this.get('type') === 'js') {
            return `Javascript is enabled for this sample and may not be needed.
                    Your spider may run faster if Javascript is not run on pages
                    like this`;
        } else {
            return `Javascript is not enabled for this sample. It may extract more 
                    accurate data if it is enabled`;
        }
    })
});
