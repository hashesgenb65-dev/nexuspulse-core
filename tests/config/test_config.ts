import { describe, it, expect, beforeEach } from 'vitest';
import {
  ConfigurationManagerEngine,
  ConfigurationError,
  BUCKET_COUNT,
  MAX_RULES_PER_FLAG,
  MAX_DESCRIPTION_LENGTH,
  type ConfigChange,
  type FeatureFlagDefinition,
} from '../../src/config/manager';

const ctx = (userId: string, segments?: string[], tenantId = 't_1') =>
  segments === undefined ? { userId, tenantId } : { userId, tenantId, segments };

/** A flag with one beta-only rule, default off. */
function betaFlag(overrides: Partial<FeatureFlagDefinition> = {}): FeatureFlagDefinition {
  return {
    flagKey: 'new_streaming_ui',
    description: 'Enables next-gen UI streaming',
    defaultEnabled: false,
    rules: [{ ruleId: 'beta_rule', targetSegments: ['beta_testers'], percentageRollout: 0, enabled: true }],
    ...overrides,
  };
}

describe('ConfigurationManagerEngine Module', () => {
  let configMgr: ConfigurationManagerEngine;

  beforeEach(() => {
    configMgr = new ConfigurationManagerEngine();
  });

  it('should store and retrieve dynamic configuration values with version tracking', () => {
    const initialVersion = configMgr.getVersion();
    configMgr.setConfig('db.timeout_ms', 5000);

    expect(configMgr.getConfig('db.timeout_ms', 1000)).toBe(5000);
    expect(configMgr.getVersion()).toBeGreaterThan(initialVersion);
  });

  it('should evaluate feature flags based on user segments and default fallbacks', () => {
    configMgr.registerFlag({
      flagKey: 'new_streaming_ui',
      description: 'Enables next-gen UI streaming',
      defaultEnabled: false,
      rules: [
        {
          ruleId: 'beta_rule',
          targetSegments: ['beta_testers'],
          percentageRollout: 0,
          enabled: true,
        },
      ],
    });

    const standardUserRes = configMgr.evaluateFlag('new_streaming_ui', {
      userId: 'usr_1',
      tenantId: 't_1',
      segments: ['standard'],
    });
    expect(standardUserRes).toBe(false);

    const betaUserRes = configMgr.evaluateFlag('new_streaming_ui', {
      userId: 'usr_2',
      tenantId: 't_1',
      segments: ['beta_testers'],
    });
    expect(betaUserRes).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Configuration values
// ---------------------------------------------------------------------------

describe('configuration values', () => {
  it('returns the default for an absent key and does not bump the version', () => {
    const before = newManager().getVersion();
    expect(newManager().getConfig('missing.key', 'fallback')).toBe('fallback');
    expect(newManager().getVersion()).toBe(before);
  });

  it('round-trips objects, arrays, null and falsy values', () => {
    const mgr = newManager();
    mgr.setConfig('a.obj', { nested: [1, { x: 'y' }] });
    mgr.setConfig('a.null', null);
    mgr.setConfig('a.zero', 0);
    mgr.setConfig('a.false', false);
    mgr.setConfig('a.empty', '');
    expect(mgr.getConfig('a.obj', null)).toEqual({ nested: [1, { x: 'y' }] });
    expect(mgr.getConfig('a.null', 'd')).toBeNull();
    expect(mgr.getConfig('a.zero', 9)).toBe(0);
    expect(mgr.getConfig('a.false', true)).toBe(false);
    expect(mgr.getConfig('a.empty', 'd')).toBe('');
  });

  it('copies values on the way in and on the way out', () => {
    const mgr = newManager();
    const input = { list: [1, 2] };
    mgr.setConfig('copy.test', input);
    input.list.push(3);
    const first = mgr.getConfig<{ list: number[] }>('copy.test', { list: [] });
    first.list.push(99);
    expect(mgr.getConfig('copy.test', { list: [] })).toEqual({ list: [1, 2] });
  });

  it.each(['', '1abc', 'has space', 'x'.repeat(129), 'bad/key', '.leading'])('rejects the key %j', (key) => {
    expectCode(() => newManager().setConfig(key, 1), 'CONFIG_INVALID_KEY');
  });

  it.each<[string, unknown]>([
    ['undefined', undefined],
    ['a function', () => 1],
    ['a symbol', Symbol('s')],
    ['a bigint', 10n],
  ])('rejects %s as a value', (_label, value) => {
    expectCode(() => newManager().setConfig('v.bad', value), 'CONFIG_INVALID_VALUE');
  });

  it('rejects a circular value', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic['self'] = cyclic;
    expectCode(() => newManager().setConfig('v.cycle', cyclic), 'CONFIG_INVALID_VALUE');
  });

  it('a rejected write leaves the previous value and version untouched', () => {
    const mgr = newManager();
    mgr.setConfig('keep', 'old');
    const version = mgr.getVersion();
    expect(() => mgr.setConfig('keep', undefined)).toThrow(ConfigurationError);
    expect(mgr.getConfig('keep', '')).toBe('old');
    expect(mgr.getVersion()).toBe(version);
  });

  it('hasConfig reports presence', () => {
    const mgr = newManager();
    expect(mgr.hasConfig('x.y')).toBe(false);
    mgr.setConfig('x.y', null);
    expect(mgr.hasConfig('x.y')).toBe(true);
  });

  it('deleteConfig removes a key, bumps the version, and is a no-op when absent', () => {
    const mgr = newManager();
    mgr.setConfig('d.k', 1);
    const afterSet = mgr.getVersion();
    expect(mgr.deleteConfig('d.k')).toBe(true);
    expect(mgr.hasConfig('d.k')).toBe(false);
    expect(mgr.getVersion()).toBe(afterSet + 1);
    const afterDelete = mgr.getVersion();
    expect(mgr.deleteConfig('d.k')).toBe(false);
    expect(mgr.getVersion()).toBe(afterDelete);
  });

  it('listConfigKeys returns sorted keys filtered by prefix', () => {
    const mgr = newManager();
    mgr.setConfig('db.timeout', 1);
    mgr.setConfig('api.port', 2);
    mgr.setConfig('db.pool', 3);
    expect(mgr.listConfigKeys()).toEqual(['api.port', 'db.pool', 'db.timeout']);
    expect(mgr.listConfigKeys('db.')).toEqual(['db.pool', 'db.timeout']);
    expect(mgr.listConfigKeys('nothing')).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Flag definitions
// ---------------------------------------------------------------------------

describe('flag definition validation', () => {
  it.each<[string, Partial<FeatureFlagDefinition>]>([
    ['an invalid flagKey', { flagKey: '1bad' }],
    ['a description that is too long', { description: 'x'.repeat(MAX_DESCRIPTION_LENGTH + 1) }],
    ['a non-boolean defaultEnabled', { defaultEnabled: 'yes' as unknown as boolean }],
    ['rules that are not an array', { rules: 'nope' as unknown as FeatureFlagDefinition['rules'] }],
  ])('rejects %s', (_label, patch) => {
    expectCode(() => newManager().registerFlag(betaFlag(patch)), 'CONFIG_INVALID_FLAG');
  });

  it('rejects too many rules', () => {
    const rules = Array.from({ length: MAX_RULES_PER_FLAG + 1 }, (_, i) => ({
      ruleId: `r${i}`,
      targetSegments: ['s'],
      percentageRollout: 0,
      enabled: true,
    }));
    expectCode(() => newManager().registerFlag(betaFlag({ rules })), 'CONFIG_INVALID_FLAG');
  });

  it.each<[string, unknown]>([
    ['a rule that is not an object', 'rule'],
    ['an invalid ruleId', { ruleId: 'bad id', targetSegments: ['s'], percentageRollout: 0, enabled: true }],
    ['a rollout above 100', { ruleId: 'r', targetSegments: ['s'], percentageRollout: 101, enabled: true }],
    ['a negative rollout', { ruleId: 'r', targetSegments: ['s'], percentageRollout: -1, enabled: true }],
    ['a NaN rollout', { ruleId: 'r', targetSegments: ['s'], percentageRollout: Number.NaN, enabled: true }],
    ['a non-boolean enabled', { ruleId: 'r', targetSegments: ['s'], percentageRollout: 5, enabled: 1 }],
    ['an invalid segment', { ruleId: 'r', targetSegments: ['has space'], percentageRollout: 0, enabled: true }],
    ['segments that are not an array', { ruleId: 'r', targetSegments: 's', percentageRollout: 5, enabled: true }],
    ['a rule that can never match', { ruleId: 'r', targetSegments: [], percentageRollout: 0, enabled: true }],
  ])('rejects a rule with %s', (_label, rule) => {
    expectCode(() => newManager().registerFlag(betaFlag({ rules: [rule as FeatureFlagRuleLike] })), 'CONFIG_INVALID_FLAG');
  });

  it('rejects duplicate rule ids within one flag', () => {
    const rules = [
      { ruleId: 'same', targetSegments: ['a'], percentageRollout: 0, enabled: true },
      { ruleId: 'same', targetSegments: ['b'], percentageRollout: 0, enabled: true },
    ];
    expectCode(() => newManager().registerFlag(betaFlag({ rules })), 'CONFIG_INVALID_FLAG');
  });

  it('accepts a rollout-only rule with no segments', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({ rules: [{ ruleId: 'pct', targetSegments: [], percentageRollout: 100, enabled: true }] }),
    );
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('anyone'))).toBe(true);
  });

  it('deduplicates segments and stores a copy, so later caller mutation has no effect', () => {
    const mgr = newManager();
    const flag = betaFlag({
      rules: [{ ruleId: 'r', targetSegments: ['beta', 'beta'], percentageRollout: 0, enabled: true }],
    });
    mgr.registerFlag(flag);
    flag.rules[0]?.targetSegments.push('other');
    flag.defaultEnabled = true;
    expect(mgr.getFlag('new_streaming_ui')?.rules[0]?.targetSegments).toEqual(['beta']);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['other']))).toBe(false);
  });

  it('re-registering a flag replaces it', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    mgr.registerFlag(betaFlag({ defaultEnabled: true, rules: [] }));
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u'))).toBe(true);
    expect(mgr.listFlags()).toEqual(['new_streaming_ui']);
  });

  it('getFlag returns a copy, and undefined for unknown flags', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    const copy = mgr.getFlag('new_streaming_ui');
    copy?.rules.push({ ruleId: 'x', targetSegments: ['y'], percentageRollout: 0, enabled: true });
    expect(mgr.getFlag('new_streaming_ui')?.rules).toHaveLength(1);
    expect(mgr.getFlag('ghost')).toBeUndefined();
  });

  it('listFlags returns sorted keys and unregisterFlag removes a flag', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag({ flagKey: 'zeta' }));
    mgr.registerFlag(betaFlag({ flagKey: 'alpha' }));
    expect(mgr.listFlags()).toEqual(['alpha', 'zeta']);
    expect(mgr.unregisterFlag('alpha')).toBe(true);
    expect(mgr.unregisterFlag('alpha')).toBe(false);
    expect(mgr.listFlags()).toEqual(['zeta']);
  });
});

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

describe('flag evaluation', () => {
  it('returns false for an unknown flag, with reason unknown-flag', () => {
    expect(newManager().explainFlag('nope', ctx('u'))).toEqual({ enabled: false, reason: 'unknown-flag' });
  });

  it('reports the segment rule that decided the result', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    expect(mgr.explainFlag('new_streaming_ui', ctx('u', ['beta_testers']))).toEqual({
      enabled: true,
      reason: 'segment',
      ruleId: 'beta_rule',
    });
  });

  it('falls back to the default with reason default when nothing matches', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag({ defaultEnabled: true }));
    expect(mgr.explainFlag('new_streaming_ui', ctx('u', ['standard']))).toEqual({ enabled: true, reason: 'default' });
  });

  it('the first matching rule in declaration order wins', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({
        rules: [
          { ruleId: 'off_for_enterprise', targetSegments: ['enterprise'], percentageRollout: 0, enabled: false },
          { ruleId: 'on_for_all_paid', targetSegments: ['enterprise', 'pro'], percentageRollout: 0, enabled: true },
        ],
      }),
    );
    expect(mgr.explainFlag('new_streaming_ui', ctx('u', ['enterprise', 'pro']))).toMatchObject({
      enabled: false,
      ruleId: 'off_for_enterprise',
    });
  });

  it('an enabled:false segment rule switches the flag off even when the default is on', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({
        defaultEnabled: true,
        rules: [{ ruleId: 'legacy_off', targetSegments: ['legacy'], percentageRollout: 0, enabled: false }],
      }),
    );
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['legacy']))).toBe(false);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['modern']))).toBe(true);
  });

  it('segment matches are case-sensitive', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['Beta_Testers']))).toBe(false);
  });

  it('treats a missing segments list the same as an empty one', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag({ defaultEnabled: true }));
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u'))).toBe(true);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', []))).toBe(true);
  });

  it('a rollout of 100 admits every user and a rollout of 0 never fires', () => {
    const everyone = newManager();
    everyone.registerFlag(
      betaFlag({ rules: [{ ruleId: 'all', targetSegments: [], percentageRollout: 100, enabled: true }] }),
    );
    for (let i = 0; i < 500; i++) {
      expect(everyone.evaluateFlag('new_streaming_ui', ctx(`u${i}`))).toBe(true);
    }

    const none = newManager();
    none.registerFlag(
      betaFlag({
        defaultEnabled: false,
        rules: [{ ruleId: 'z', targetSegments: ['s'], percentageRollout: 0, enabled: true }],
      }),
    );
    for (let i = 0; i < 500; i++) {
      expect(none.evaluateFlag('new_streaming_ui', ctx(`u${i}`))).toBe(false);
    }
  });

  it('a 50% rollout admits about half of 10,000 users', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({ rules: [{ ruleId: 'half', targetSegments: [], percentageRollout: 50, enabled: true }] }),
    );
    let on = 0;
    for (let i = 0; i < 10_000; i++) {
      if (mgr.evaluateFlag('new_streaming_ui', ctx(`user-${i}`))) on++;
    }
    expect(on / 10_000).toBeGreaterThan(0.47);
    expect(on / 10_000).toBeLessThan(0.53);
  });

  it('a 0.5% rollout admits roughly 5 users in 1,000 and respects the bucket granularity', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({ rules: [{ ruleId: 'tiny', targetSegments: [], percentageRollout: 0.5, enabled: true }] }),
    );
    let on = 0;
    for (let i = 0; i < 20_000; i++) {
      if (mgr.evaluateFlag('new_streaming_ui', ctx(`acct-${i}`))) on++;
    }
    expect(on / 20_000).toBeGreaterThan(0.003);
    expect(on / 20_000).toBeLessThan(0.007);
    expect(BUCKET_COUNT).toBe(10_000);
  });

  it('rollout cohorts are sticky across repeated evaluations', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({ rules: [{ ruleId: 'half', targetSegments: [], percentageRollout: 50, enabled: true }] }),
    );
    for (let i = 0; i < 200; i++) {
      const first = mgr.evaluateFlag('new_streaming_ui', ctx(`u${i}`));
      for (let k = 0; k < 3; k++) {
        expect(mgr.evaluateFlag('new_streaming_ui', ctx(`u${i}`))).toBe(first);
      }
    }
  });

  it('cohorts differ between flags, so one flag does not always pick the same users', () => {
    const mgr = newManager();
    const rule = { ruleId: 'half', targetSegments: [], percentageRollout: 50, enabled: true };
    mgr.registerFlag(betaFlag({ flagKey: 'flag_a', rules: [rule] }));
    mgr.registerFlag(betaFlag({ flagKey: 'flag_b', rules: [rule] }));
    let differing = 0;
    for (let i = 0; i < 400; i++) {
      const a = mgr.evaluateFlag('flag_a', ctx(`user-${i}`));
      const b = mgr.evaluateFlag('flag_b', ctx(`user-${i}`));
      if (a !== b) differing++;
    }
    expect(differing).toBeGreaterThan(100);
  });

  it('rules are evaluated in order, so an earlier rollout shadows a later segment rule', () => {
    const mgr = newManager();
    mgr.registerFlag(
      betaFlag({
        rules: [
          { ruleId: 'pct', targetSegments: [], percentageRollout: 100, enabled: true },
          { ruleId: 'seg_off', targetSegments: ['vip'], percentageRollout: 0, enabled: false },
        ],
      }),
    );
    // The rollout rule comes first and matches everyone, so the VIP rule is never reached.
    expect(mgr.explainFlag('new_streaming_ui', ctx('u', ['vip']))).toMatchObject({ reason: 'rollout' });
  });

  it.each<[string, unknown]>([
    ['a missing userId', { tenantId: 't' }],
    ['an empty userId', { userId: '', tenantId: 't' }],
    ['a userId with spaces', { userId: 'a b', tenantId: 't' }],
    ['a missing tenantId', { userId: 'u' }],
    ['segments that are not an array', { userId: 'u', tenantId: 't', segments: 'beta' }],
    ['a non-string segment', { userId: 'u', tenantId: 't', segments: [1] }],
    ['an empty segment', { userId: 'u', tenantId: 't', segments: [''] }],
    ['a non-object context', 'u'],
  ])('rejects a context with %s', (_label, context) => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    expectCode(() => mgr.evaluateFlag('new_streaming_ui', context as never), 'CONFIG_INVALID_CONTEXT');
  });
});

// ---------------------------------------------------------------------------
// Tenant overrides
// ---------------------------------------------------------------------------

describe('tenant overrides', () => {
  it('a tenant override beats every rule and the default', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag({ defaultEnabled: true }));
    mgr.setTenantOverride('new_streaming_ui', 'tenant-x', false);
    expect(mgr.explainFlag('new_streaming_ui', ctx('u', ['beta_testers'], 'tenant-x'))).toEqual({
      enabled: false,
      reason: 'tenant-override',
    });
  });

  it('an override applies only to its own tenant', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    mgr.setTenantOverride('new_streaming_ui', 'tenant-x', true);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['standard'], 'tenant-x'))).toBe(true);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', ['standard'], 'tenant-y'))).toBe(false);
  });

  it('setTenantOverride requires a registered flag', () => {
    expectCode(() => newManager().setTenantOverride('ghost', 't', true), 'CONFIG_UNKNOWN_FLAG');
  });

  it('setTenantOverride rejects an invalid tenant id or a non-boolean value', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    expectCode(() => mgr.setTenantOverride('new_streaming_ui', 'bad tenant', true), 'CONFIG_INVALID_TENANT');
    expectCode(
      () => mgr.setTenantOverride('new_streaming_ui', 't', 'yes' as unknown as boolean),
      'CONFIG_INVALID_VALUE',
    );
  });

  it('clearTenantOverride removes the override and reports whether one existed', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag({ defaultEnabled: true }));
    mgr.setTenantOverride('new_streaming_ui', 'tenant-x', false);
    expect(mgr.clearTenantOverride('new_streaming_ui', 'tenant-x')).toBe(true);
    expect(mgr.clearTenantOverride('new_streaming_ui', 'tenant-x')).toBe(false);
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', [], 'tenant-x'))).toBe(true);
  });

  it('unregistering a flag also drops its overrides', () => {
    const mgr = newManager();
    mgr.registerFlag(betaFlag());
    mgr.setTenantOverride('new_streaming_ui', 'tenant-x', true);
    mgr.unregisterFlag('new_streaming_ui');
    mgr.registerFlag(betaFlag());
    expect(mgr.evaluateFlag('new_streaming_ui', ctx('u', [], 'tenant-x'))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Change notification
// ---------------------------------------------------------------------------

describe('change notification', () => {
  it('reports every mutation with the new version', () => {
    const mgr = newManager();
    const seen: ConfigChange[] = [];
    mgr.onChange((c) => seen.push(c));

    mgr.setConfig('k', 1);
    mgr.deleteConfig('k');
    mgr.registerFlag(betaFlag());
    mgr.setTenantOverride('new_streaming_ui', 't', true);
    mgr.clearTenantOverride('new_streaming_ui', 't');
    mgr.unregisterFlag('new_streaming_ui');

    expect(seen.map((c) => `${c.kind}:${'action' in c ? c.action : ''}`)).toEqual([
      'config:set',
      'config:delete',
      'flag:register',
      'override:set',
      'override:clear',
      'flag:unregister',
    ]);
    expect(seen.map((c) => c.version)).toEqual([2, 3, 4, 5, 6, 7]);
    expect(mgr.getVersion()).toBe(7);
  });

  it('a failed mutation does not notify', () => {
    const mgr = newManager();
    const seen: ConfigChange[] = [];
    mgr.onChange((c) => seen.push(c));
    expect(() => mgr.registerFlag(betaFlag({ defaultEnabled: 'x' as unknown as boolean }))).toThrow();
    expect(() => mgr.setConfig('bad key', 1)).toThrow();
    expect(seen).toHaveLength(0);
  });

  it('unsubscribe stops notifications', () => {
    const mgr = newManager();
    let calls = 0;
    const off = mgr.onChange(() => calls++);
    mgr.setConfig('a', 1);
    off();
    mgr.setConfig('b', 2);
    expect(calls).toBe(1);
  });

  it('a throwing listener is counted and does not stop the others', () => {
    const mgr = newManager();
    let good = 0;
    mgr.onChange(() => {
      throw new Error('listener broke');
    });
    mgr.onChange(() => good++);
    expect(() => mgr.setConfig('a', 1)).not.toThrow();
    expect(good).toBe(1);
    expect(mgr.listenerErrors).toBe(1);
    expect(mgr.getConfig('a', 0)).toBe(1);
  });

  it('rejects a non-function listener', () => {
    expect(() => newManager().onChange('nope' as unknown as () => void)).toThrow(TypeError);
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type FeatureFlagRuleLike = FeatureFlagDefinition['rules'][number];

function newManager(): ConfigurationManagerEngine {
  return new ConfigurationManagerEngine();
}

function expectCode(fn: () => unknown, code: string): void {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(ConfigurationError);
    expect((err as ConfigurationError).code).toBe(code);
    return;
  }
  throw new Error(`expected ${code} to be thrown`);
}
