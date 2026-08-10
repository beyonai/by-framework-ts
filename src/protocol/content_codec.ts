import { WireContent } from './results';

export type { WireContent };

export interface ContentCodec {
    serialize(content: unknown): WireContent;
    deserialize(content: WireContent): unknown;
}
