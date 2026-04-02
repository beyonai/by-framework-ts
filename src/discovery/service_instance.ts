/**
 * Service instance information data structure.
 */
export interface ServiceInstanceData {
    id: string;
    host: string;
    port: number;
    weight: number;
    metadata: Record<string, unknown>;
}

export class ServiceInstance {
    constructor(
        public readonly id: string,
        public readonly host: string,
        public readonly port: number,
        public readonly weight: number = 1,
        public readonly metadata: Record<string, unknown> = {}
    ) {}

    toJSON(): string {
        return JSON.stringify({
            id: this.id,
            host: this.host,
            port: this.port,
            weight: this.weight,
            metadata: this.metadata,
        });
    }

    static fromJSON(data: string): ServiceInstance {
        const parsed = JSON.parse(data) as ServiceInstanceData;
        return new ServiceInstance(
            parsed.id,
            parsed.host,
            parsed.port,
            parsed.weight,
            parsed.metadata
        );
    }

    toDict(): ServiceInstanceData {
        return {
            id: this.id,
            host: this.host,
            port: this.port,
            weight: this.weight,
            metadata: this.metadata,
        };
    }
}