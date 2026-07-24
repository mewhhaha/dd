type KvNamespace = {
  get(key: string): Promise<unknown | null>;
  put(key: string, value: unknown): void | Promise<void>;
  delete(key: string): void | Promise<void>;
};

type MemoryNamespace = {
  idFromName(name: string): unknown;
  get(id: unknown): MemoryShard;
};

type MemoryShard = {
  atomic<T>(callback: () => T): Promise<T>;
  tvar<T>(key: string, defaultValue: T): {
    read(): T;
    write(value: T): void;
  };
};
