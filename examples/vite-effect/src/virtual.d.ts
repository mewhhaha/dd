declare module "virtual:dd-auxiliary-workers" {
  export type AuxiliaryWorkerRecord = {
    name: string;
    kind: "dynamic" | "service";
    binding: string;
    id: string;
    service?: string;
    config: Record<string, unknown>;
  };

  export const workers: Record<string, AuxiliaryWorkerRecord>;
  export default workers;
}
