import { buildObservabilitySnapshot } from '../src/metrics/snapshot';
import { RegistryKeys } from '../src/constants';

describe('buildObservabilitySnapshot worker discovery under v2 hash-tagged keys', () => {
    const originalValue = process.env.REDIS_KEY_SCHEMA_VERSION;

    afterEach(() => {
        if (originalValue === undefined) {
            delete process.env.REDIS_KEY_SCHEMA_VERSION;
        } else {
            process.env.REDIS_KEY_SCHEMA_VERSION = originalValue;
        }
    });

    test('finds all online workers when worker_id is wrapped in a Cluster hash tag mid-key', async () => {
        process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';

        const onlineKeys = [
            RegistryKeys.worker_online_lease('worker-01'),
            RegistryKeys.worker_online_lease('worker-02'),
        ];

        const fakeRedis: any = {
            scan: jest.fn().mockResolvedValue(['0', onlineKeys]),
            smembers: jest.fn().mockResolvedValue([]),
            xlen: jest.fn().mockResolvedValue(0),
        };

        const snapshot = await buildObservabilitySnapshot(fakeRedis);

        expect(snapshot.totals.workers_online).toBe(2);
        const [, , pattern] = fakeRedis.scan.mock.calls[0];
        expect(pattern).toBe('byai_gateway:v2:registry:worker:{*}:online');
    });

    test('still finds workers under v1 (default) unprefixed keys', async () => {
        delete process.env.REDIS_KEY_SCHEMA_VERSION;

        const onlineKeys = [
            RegistryKeys.worker_online_lease('worker-01'),
            RegistryKeys.worker_online_lease('worker-02'),
        ];

        const fakeRedis: any = {
            scan: jest.fn().mockResolvedValue(['0', onlineKeys]),
            smembers: jest.fn().mockResolvedValue([]),
            xlen: jest.fn().mockResolvedValue(0),
        };

        const snapshot = await buildObservabilitySnapshot(fakeRedis);

        expect(snapshot.totals.workers_online).toBe(2);
    });
});
