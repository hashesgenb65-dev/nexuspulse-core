import { describe, it, expect, beforeEach } from 'vitest';
import { WorkflowDagEngine } from '../../src/orchestrator/dag';

describe('WorkflowDagEngine Module', () => {
  let dag: WorkflowDagEngine;

  beforeEach(() => {
    dag = new WorkflowDagEngine();
  });

  it('should execute tasks in topological DAG dependency order', async () => {
    const executionLog: string[] = [];

    dag.addTask('task_a', 'Extract Data', [], async () => {
      executionLog.push('A');
      return { extracted: true };
    });

    dag.addTask('task_b', 'Transform Data', ['task_a'], async () => {
      executionLog.push('B');
      return { transformed: true };
    });

    dag.addTask('task_c', 'Load Data', ['task_b'], async () => {
      executionLog.push('C');
      return { loaded: true };
    });

    const result = await dag.executeWorkflow();

    expect(result.success).toBe(true);
    expect(executionLog).toEqual(['A', 'B', 'C']);
    expect(dag.getTaskStatus('task_c')).toBe('SUCCESS');
  });

  it('should fail workflow execution when a parent dependency fails', async () => {
    dag.addTask('task_1', 'Failing Root', [], async () => {
      throw new Error('Database connection failed');
    });

    dag.addTask('task_2', 'Dependent Task', ['task_1'], async () => {
      return { ok: true };
    });

    const result = await dag.executeWorkflow();

    expect(result.success).toBe(false);
    expect(result.failedTaskId).toBe('task_1');
    expect(dag.getTaskStatus('task_2')).toBe('PENDING'); // Skipped/Pending due to dependency failure
  });
});
