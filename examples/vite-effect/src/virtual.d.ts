declare module "virtual:dd-auxiliary-workers" {
  export type AuxiliaryWorkerRecord = {
    name: string;
    binding: string;
    id: string;
    config: import("./types").DynamicWorkerConfig;
  };

  export const workers: Record<string, AuxiliaryWorkerRecord>;
  export default workers;
}
