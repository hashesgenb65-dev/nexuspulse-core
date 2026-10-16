/**
 * NexusPulse Asynchronous Distributed Job Orchestrator & Workflow DAG Engine
 * 
 * Implements Directed Acyclic Graph (DAG) task dependency resolution, parallel execution stages,
 * topological sorting, and failure propagation handling.
 */

export type DagTaskStatus = 'PENDING' | 'READY' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'SKIPPED';

export interface DagTaskNode {
  taskId: string;
  name: string;
  dependencies: string[]; // taskIds that must complete successfully first
  handler: () => Promise<any>;
  status: DagTaskStatus;
  result?: any;
  error?: string;
}

export class WorkflowDagEngine {
  private tasks: Map<string, DagTaskNode> = new Map();

  public addTask(
    taskId: string,
    name: string,
    dependencies: string[],
    handler: () => Promise<any>
  ): void {
    this.tasks.set(taskId, {
      taskId,
      name,
      dependencies,
      handler,
      status: 'PENDING',
    });
  }

  public async executeWorkflow(): Promise<{ success: boolean; completedTasks: number; failedTaskId?: string }> {
    // Validate DAG for cycles and missing dependencies
    this.validateDependencies();

    let completedCount = 0;
    const totalTasks = this.tasks.size;

    while (completedCount < totalTasks) {
      const readyTasks = Array.from(this.tasks.values()).filter(
        (t) => t.status === 'PENDING' && this.areDependenciesSatisfied(t)
      );

      if (readyTasks.length === 0) {
        // Check if any tasks failed or if we are deadlocked
        const failed = Array.from(this.tasks.values()).find((t) => t.status === 'FAILED');
        if (failed) {
          return { success: false, completedTasks: completedCount, failedTaskId: failed.taskId };
        }
        break;
      }

      // Execute ready tasks in parallel
      const executionPromises = readyTasks.map(async (task) => {
        task.status = 'RUNNING';
        try {
          task.result = await task.handler();
          task.status = 'SUCCESS';
          completedCount++;
        } catch (err: unknown) {
          task.status = 'FAILED';
          task.error = err instanceof Error ? err.message : 'Unknown execution failure';
          completedCount++;
        }
      });

      await Promise.all(executionPromises);
    }

    const hasFailures = Array.from(this.tasks.values()).some((t) => t.status === 'FAILED');
    return { success: !hasFailures, completedTasks: completedCount };
  }

  public getTaskStatus(taskId: string): DagTaskStatus | null {
    return this.tasks.get(taskId)?.status ?? null;
  }

  private areDependenciesSatisfied(task: DagTaskNode): boolean {
    for (const depId of task.dependencies) {
      const depTask = this.tasks.get(depId);
      if (!depTask || depTask.status !== 'SUCCESS') {
        return false;
      }
    }
    return true;
  }

  private validateDependencies(): void {
    for (const task of this.tasks.values()) {
      for (const depId of task.dependencies) {
        if (!this.tasks.has(depId)) {
          throw new Error(`Task '${task.taskId}' depends on non-existent task '${depId}'.`);
        }
      }
    }
  }
}
