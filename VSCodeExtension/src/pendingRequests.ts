type Pending<T> = {
  resolve: (value: T) => void;
  reject: (reason: Error) => void;
  timer: NodeJS.Timeout;
};

export class PendingRequests<T> {
  private readonly requests = new Map<string, Pending<T>>();

  public async run(key: string, timeout: number, timeoutMessage: string, dispatch: () => Promise<void>): Promise<T> {
    const completion = this.wait(key, timeout, timeoutMessage);
    const pending = this.requests.get(key);
    // Attach the rejection handler before dispatch: the notification or timeout can
    // arrive while the transport is still waiting for the request acknowledgement.
    void Promise.resolve().then(dispatch).catch(error => {
      if (this.requests.get(key) === pending) { this.reject(key, error); }
    });
    return await completion;
  }

  public wait(key: string, timeout: number, timeoutMessage: string): Promise<T> {
    this.reject(key, new Error("Superseded by a newer request."));
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.requests.delete(key);
        reject(new Error(timeoutMessage));
      }, timeout);
      this.requests.set(key, { resolve, reject, timer });
    });
  }

  public resolve(key: string, value: T): boolean {
    const pending = this.take(key);
    if (!pending) { return false; }
    pending.resolve(value);
    return true;
  }

  public reject(key: string, reason: unknown): boolean {
    const pending = this.take(key);
    if (!pending) { return false; }
    pending.reject(reason instanceof Error ? reason : new Error(String(reason)));
    return true;
  }

  public rejectAll(reason: unknown): void {
    for (const key of [...this.requests.keys()]) { this.reject(key, reason); }
  }

  private take(key: string): Pending<T> | undefined {
    const pending = this.requests.get(key);
    if (!pending) { return undefined; }
    clearTimeout(pending.timer);
    this.requests.delete(key);
    return pending;
  }
}
