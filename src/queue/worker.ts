/**
 * NexusPulse Distributed Event Bus & Asynchronous Worker Queue Engine
 * 
 * Implements priority-weighted task scheduling, asynchronous worker thread pooling,
 * exponential backoff retry strategies, circuit breaking, and dead-letter queue recovery.
 */

export type TaskPriority = 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL';
export type TaskState = 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'RETRYING' | 'DEAD';

export interface QueueTask<T = unknown> {
  taskId: string;
  name: string;
  payload: T;
  priority: TaskPriority;
  maxRetries: number;
  retryCount: number;
  backoffMs: number;
  state: TaskState;
  createdAt: number;
  updatedAt: number;
  error?: string;
}

export type TaskHandler<T = unknown, R = unknown> = (task: QueueTask<T>) => Promise<R>;

export interface QueueConfig {
  maxConcurrentWorkers: number;
  defaultMaxRetries: number;
  baseBackoffMs: number;
  circuitBreakerFailureThreshold: number;
}

export class WorkerQueueEngine {
  private queue: QueueTask[] = [];
  private processing: Map<string, QueueTask> = new Map();
  private completed: Map<string, QueueTask> = new Map();
  private deadLetter: QueueTask[] = [];
  private handlers: Map<string, TaskHandler> = new Map();
  private config: QueueConfig;
  private activeWorkers: number = 0;
  private isRunning: boolean = false;
  private consecutiveFailures: number = 0;
  private circuitOpen: boolean = false;
  private processingScheduled: boolean = false;

  constructor(config: QueueConfig = {
    maxConcurrentWorkers: 4,
    defaultMaxRetries: 3,
    baseBackoffMs: 1000,
    circuitBreakerFailureThreshold: 10,
  }) {
    this.config = config;
  }

  /**
   * Register an execution handler function for a specific task name.
   */
  public registerHandler<T, R>(taskName: string, handler: TaskHandler<T, R>): void {
    this.handlers.set(taskName, handler as TaskHandler);
  }

  /**
   * Enqueue a new asynchronous task with priority weighting.
   */
  public enqueue<T>(
    name: string,
    payload: T,
    priority: TaskPriority = 'NORMAL',
    maxRetries?: number
  ): string {
    const taskId = `tsk_${Math.random().toString(36).substring(2, 11)}`;
    const task: QueueTask<T> = {
      taskId,
      name,
      payload,
      priority,
      maxRetries: maxRetries ?? this.config.defaultMaxRetries,
      retryCount: 0,
      backoffMs: this.config.baseBackoffMs,
      state: 'PENDING',
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };

    this.queue.push(task as QueueTask);
    this.sortQueueByPriority();
    this.scheduleProcessing();

    return taskId;
  }

  /**
   * Start queue worker processing loops.
   */
  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.triggerProcessing();
  }

  /**
   * Stop worker queue gracefully.
   */
  public stop(): void {
    this.isRunning = false;
  }

  /**
   * Retrieve queue metrics and operational status.
   */
  public getMetrics(): {
    pending: number;
    processing: number;
    completed: number;
    deadLetter: number;
    activeWorkers: number;
    circuitOpen: boolean;
  } {
    return {
      pending: this.queue.length,
      processing: this.processing.size,
      completed: this.completed.size,
      deadLetter: this.deadLetter.length,
      activeWorkers: this.activeWorkers,
      circuitOpen: this.circuitOpen,
    };
  }

  private sortQueueByPriority(): void {
    const weights: Record<TaskPriority, number> = {
      CRITICAL: 4,
      HIGH: 3,
      NORMAL: 2,
      LOW: 1,
    };

    this.queue.sort((a, b) => weights[b.priority] - weights[a.priority]);
  }

  /**
   * Dispatch on a microtask, so a synchronous burst of enqueue calls is sorted by
   * priority before any task starts. Without this, the first task would start
   * immediately and priority would only affect tasks that were already waiting.
   */
  private scheduleProcessing(): void {
    if (this.processingScheduled) return;
    this.processingScheduled = true;
    queueMicrotask(() => {
      this.processingScheduled = false;
      this.triggerProcessing();
    });
  }

  private triggerProcessing(): void {
    if (!this.isRunning || this.circuitOpen) return;

    while (
      this.activeWorkers < this.config.maxConcurrentWorkers &&
      this.queue.length > 0 &&
      !this.circuitOpen
    ) {
      const task = this.queue.shift();
      if (!task) break;

      this.activeWorkers++;
      this.executeTask(task);
    }
  }

  private async executeTask(task: QueueTask): Promise<void> {
    task.state = 'RUNNING';
    task.updatedAt = Date.now();
    this.processing.set(task.taskId, task);

    const handler = this.handlers.get(task.name);

    try {
      if (!handler) {
        throw new Error(`No registered handler found for task type: '${task.name}'`);
      }

      await handler(task);

      // Success
      task.state = 'COMPLETED';
      task.updatedAt = Date.now();
      this.processing.delete(task.taskId);
      this.completed.set(task.taskId, task);
      this.consecutiveFailures = 0; // Reset circuit breaker counter
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown execution failure';
      task.error = errorMessage;
      this.processing.delete(task.taskId);

      if (task.retryCount < task.maxRetries) {
        task.retryCount++;
        task.state = 'RETRYING';
        task.backoffMs *= 2; // Exponential backoff
        task.updatedAt = Date.now();

        // Re-queue after backoff delay
        setTimeout(() => {
          this.queue.push(task);
          this.sortQueueByPriority();
          this.triggerProcessing();
        }, task.backoffMs);
      } else {
        // Max retries exceeded -> Dead Letter Queue
        task.state = 'DEAD';
        task.updatedAt = Date.now();
        this.deadLetter.push(task);
        this.consecutiveFailures++;

        if (this.consecutiveFailures >= this.config.circuitBreakerFailureThreshold) {
          this.circuitOpen = true;
        }
      }
    } finally {
      this.activeWorkers--;
      this.triggerProcessing();
    }
  }
}
