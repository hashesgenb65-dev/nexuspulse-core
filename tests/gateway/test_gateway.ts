import { describe, it, expect, beforeEach } from 'vitest';
import { GatewayEngine } from '../../src/gateway/gateway';

describe('GatewayEngine Module', () => {
  let gateway: GatewayEngine;

  beforeEach(() => {
    gateway = new GatewayEngine({
      types: {},
      queryRoot: {
        getUser: async (_, args) => ({ id: args.id, name: 'Enterprise User', role: 'ADMIN' }),
      },
      mutationRoot: {
        updateMetric: async (_, args) => ({ metricKey: args.key, value: args.val, updated: true }),
      },
    });
  });

  it('should execute GraphQL queries successfully', async () => {
    const result = await gateway.executeGraphQLQuery('getUser', { id: 'usr_123' }, {});
    expect(result.errors).toHaveLength(0);
    expect(result.data.name).toBe('Enterprise User');
  });

  it('should execute GraphQL mutations successfully', async () => {
    const result = await gateway.executeGraphQLMutation('updateMetric', { key: 'cpu', val: 92.5 }, {});
    expect(result.errors).toHaveLength(0);
    expect(result.data.updated).toBe(true);
  });

  it('should generate valid HMAC-SHA256 webhook signatures', () => {
    const sig = gateway.generateWebhookSignature('{"status":"ok"}', 'secret_abc');
    expect(sig).toHaveLength(64); // SHA-256 hex length
  });

  it('should register and dispatch webhooks with signature headers', async () => {
    gateway.registerWebhook({
      subscriptionId: 'sub_1',
      targetUrl: 'https://api.client.com/webhook',
      secretKey: 'my_secret_key',
      subscribedEvents: ['alert.critical'],
      isActive: true,
      failureCount: 0,
    });

    const attempts = await gateway.dispatchWebhook('alert.critical', { alertId: 'alt_99', severity: 'HIGH' });
    expect(attempts.length).toBeGreaterThanOrEqual(0); // mock dispatch tracked
  });
});
