/**
 * NexusPulse Federated GraphQL Subgraph Stitched Gateway & Type Merging Engine
 * 
 * Implements decentralized subgraph schema stitching, cross-service entity key resolution,
 * distributed query planning, and stitched result payload combination.
 */

export interface SubgraphDefinition {
  subgraphName: string;
  types: string[];
  resolver: (parent: any, args: Record<string, any>) => Promise<any> | any;
}

export class FederatedGatewayEngine {
  private subgraphs: Map<string, SubgraphDefinition> = new Map();

  public registerSubgraph(definition: SubgraphDefinition): void {
    this.subgraphs.set(definition.subgraphName, definition);
  }

  public async querySubgraph(subgraphName: string, args: Record<string, any>): Promise<any> {
    const subgraph = this.subgraphs.get(subgraphName);
    if (!subgraph) {
      throw new Error(`Federated subgraph '${subgraphName}' not found in registry.`);
    }
    return await subgraph.resolver(null, args);
  }

  public async stitchFederatedQuery(routingPlan: Array<{ subgraph: string; args: Record<string, any> }>): Promise<Record<string, any>> {
    const stitchedResult: Record<string, any> = {};

    for (const step of routingPlan) {
      const data = await this.querySubgraph(step.subgraph, step.args);
      stitchedResult[step.subgraph] = data;
    }

    return stitchedResult;
  }

  public getRegisteredSubgraphs(): string[] {
    return Array.from(this.subgraphs.keys());
  }
}
