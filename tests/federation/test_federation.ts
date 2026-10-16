import { describe, it, expect, beforeEach } from 'vitest';
import { FederatedGatewayEngine } from '../../src/federation/stitch';

describe('FederatedGatewayEngine Module', () => {
  let gateway: FederatedGatewayEngine;

  beforeEach(() => {
    gateway = new FederatedGatewayEngine();
  });

  it('should register subgraphs and execute individual subgraph queries', async () => {
    gateway.registerSubgraph({
      subgraphName: 'users-service',
      types: ['User'],
      resolver: async (_, args) => ({ id: args.id, username: 'enterprise_dev' }),
    });

    const res = await gateway.querySubgraph('users-service', { id: 'u_1' });
    expect(res.username).toBe('enterprise_dev');
  });

  it('should stitch multi-subgraph federated queries into unified payloads', async () => {
    gateway.registerSubgraph({
      subgraphName: 'auth-subgraph',
      types: ['Session'],
      resolver: async () => ({ token: 'jwt_secure_token' }),
    });

    gateway.registerSubgraph({
      subgraphName: 'telemetry-subgraph',
      types: ['Metrics'],
      resolver: async () => ({ activeNodes: 12 }),
    });

    const stitched = await gateway.stitchFederatedQuery([
      { subgraph: 'auth-subgraph', args: {} },
      { subgraph: 'telemetry-subgraph', args: {} },
    ]);

    expect(stitched['auth-subgraph'].token).toBe('jwt_secure_token');
    expect(stitched['telemetry-subgraph'].activeNodes).toBe(12);
  });
});
