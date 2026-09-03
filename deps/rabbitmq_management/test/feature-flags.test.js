import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  feature_flags_refresh,
  enable_all_stable_feature_flags,
  lookup_feature_flag
} from '../priv/www/js/feature-flags.js';

describe('feature-flags: refresh & lookup', () => {
  let mockElements;

  beforeEach(() => {
    mockElements = new Map();

    globalThis.document = {
      getElementById: (id) => mockElements.get(id) || null
    };
  });

  it('resets nonreq_feature_flags when data element is missing', () => {
    feature_flags_refresh();
    assert.equal(lookup_feature_flag('any_flag'), undefined);
  });

  it('parses feature flags dataset and identifies disabled stable flags', () => {
    const flagsData = [
      { name: 'user_limits', state: 'disabled', stability: 'stable' },
      { name: 'khepri_db', state: 'disabled', stability: 'experimental' },
      { name: 'container_limits', state: 'enabled', stability: 'stable' }
    ];

    const mockDataElem = {
      dataset: {
        featureFlags: JSON.stringify(flagsData)
      }
    };
    const mockButton = { disabled: true };
    const mockWarning = { style: { display: 'none' } };

    mockElements.set('ff-feature-flags-data', mockDataElem);
    mockElements.set('ff-enable-all-button', mockButton);
    mockElements.set('ff-disabled-stable-warning', mockWarning);

    feature_flags_refresh();

    // Check lookup_feature_flag
    assert.deepEqual(lookup_feature_flag('user_limits'), flagsData[0]);
    assert.deepEqual(lookup_feature_flag('khepri_db'), flagsData[1]);
    assert.equal(lookup_feature_flag('non_existent'), undefined);

    // Stable disabled flag should enable the "Enable All" button & show warning
    assert.equal(mockButton.disabled, false);
    assert.equal(mockWarning.style.display, 'block');
  });

  it('dispatches change events for disabled stable feature flags', () => {
    const flagsData = [
      { name: 'stable_flag_1', state: 'disabled', stability: 'stable' },
      { name: 'exp_flag_1', state: 'disabled', stability: 'experimental' }
    ];

    mockElements.set('ff-feature-flags-data', {
      dataset: { featureFlags: JSON.stringify(flagsData) }
    });
    mockElements.set('ff-enable-all-button', { disabled: true });
    mockElements.set('ff-disabled-stable-warning', { style: { display: 'none' } });

    const dispatchedEvents = [];
    const mockCheckbox1 = {
      disabled: false,
      dispatchEvent: (evt) => dispatchedEvents.push(['stable_flag_1', evt.type])
    };
    const mockCheckbox2 = {
      disabled: false,
      dispatchEvent: (evt) => dispatchedEvents.push(['exp_flag_1', evt.type])
    };

    mockElements.set('ff-checkbox-stable_flag_1', mockCheckbox1);
    mockElements.set('ff-checkbox-exp_flag_1', mockCheckbox2);

    globalThis.Event = class Event {
      constructor(type, opts) {
        this.type = type;
        this.opts = opts;
      }
    };

    feature_flags_refresh();

    const mockButton = { disabled: false };
    enable_all_stable_feature_flags(mockButton);

    assert.equal(mockButton.disabled, true);
    assert.deepEqual(dispatchedEvents, [['stable_flag_1', 'change']]);
  });
});
