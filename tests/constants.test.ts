import { QueueNames, RegistryKeys } from '../src/constants';

/**
 * Redis Cluster CRC16 (XMODEM variant) + hash-tag slot resolution, matching
 * Redis's own algorithm (see Redis's cluster.c keyHashSlot). Reimplemented
 * here (rather than pulled from a dependency) purely for test-time slot
 * verification.
 */
function crc16(str: string): number {
    let crc = 0;
    for (let i = 0; i < str.length; i++) {
        crc ^= str.charCodeAt(i) << 8;
        for (let j = 0; j < 8; j++) {
            crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1);
            crc &= 0xffff;
        }
    }
    return crc;
}

function hashTag(key: string): string {
    const start = key.indexOf('{');
    if (start === -1) return key;
    const end = key.indexOf('}', start + 1);
    if (end === -1 || end === start + 1) return key;
    return key.slice(start + 1, end);
}

function slot(key: string): number {
    return crc16(hashTag(key)) % 16384;
}

describe('constants v1/v2 key schema', () => {
    const originalValue = process.env.REDIS_KEY_SCHEMA_VERSION;

    afterEach(() => {
        if (originalValue === undefined) {
            delete process.env.REDIS_KEY_SCHEMA_VERSION;
        } else {
            process.env.REDIS_KEY_SCHEMA_VERSION = originalValue;
        }
    });

    describe('v1 (default) — byte-identical to pre-versioning format', () => {
        beforeEach(() => {
            delete process.env.REDIS_KEY_SCHEMA_VERSION;
        });

        test('QueueNames methods are unchanged', () => {
            expect(QueueNames.ctrl_stream('chat')).toBe('byai_gateway:ctrl:agent_type:chat');
            expect(QueueNames.worker_ctrl_stream('worker-01')).toBe('byai_gateway:ctrl:worker:worker-01');
            expect(QueueNames.session_data_stream('sess-abc123')).toBe(
                'byai_gateway:session:sess-abc123:data_stream'
            );
            expect(QueueNames.task_group('tg-1')).toBe('byai_gateway:task_group:tg-1');
            expect(QueueNames.task_group_results('tg-1')).toBe('byai_gateway:task_group:tg-1:results');
            expect(QueueNames.trace_meta('trace-xyz')).toBe('byai_gateway:trace:trace-xyz:meta');
            expect(QueueNames.trace_spans('trace-xyz')).toBe('byai_gateway:trace:trace-xyz:spans');
            expect(QueueNames.trace_index_session('sess-abc123')).toBe(
                'byai_gateway:trace:idx:session:sess-abc123'
            );
            expect(QueueNames.trace_index_worker('worker-01')).toBe('byai_gateway:trace:idx:worker:worker-01');
            expect(QueueNames.trace_index_agent('chat')).toBe('byai_gateway:trace:idx:agent:chat');
        });

        test('RegistryKeys methods are unchanged', () => {
            expect(RegistryKeys.known_workers()).toBe('byai_gateway:registry:workers');
            expect(RegistryKeys.sd_active_instances('svc-a')).toBe('byai_gateway:sd:active:svc-a');
            expect(RegistryKeys.sd_instance_details('svc-a')).toBe('byai_gateway:sd:instances:svc-a');
            expect(RegistryKeys.sd_services()).toBe('byai_gateway:sd:services');
            expect(RegistryKeys.worker_online_lease('worker-01')).toBe(
                'byai_gateway:registry:worker:online:worker-01'
            );
            expect(RegistryKeys.workerDeclaredAgentTypes('worker-01')).toBe(
                'byai_gateway:registry:worker:agent_types:worker-01'
            );
            expect(RegistryKeys.agentTypeMembers('chat')).toBe('byai_gateway:registry:agent_type:workers:chat');
            expect(RegistryKeys.workerAdminState('worker-01')).toBe('byai_gateway:registry:worker:admin:worker-01');
            expect(RegistryKeys.agentTypeDenied('chat')).toBe('byai_gateway:registry:agent_type:denied:chat');
            expect(RegistryKeys.worker_lock('worker-01')).toBe('byai_gateway:registry:worker:lock:worker-01');
            expect(RegistryKeys.session_registry('sess-abc123')).toBe('byai_gateway:session:sess-abc123:registry');
        });
    });

    describe('v2 golden keys — fixed IDs -> exact key strings', () => {
        beforeEach(() => {
            process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';
        });

        const ids = {
            session_id: 'sess-abc123',
            worker_id: 'worker-01',
            trace_id: 'trace-xyz',
            agent_type: 'chat',
            group_id: 'tg-1',
            service_name: 'svc-a',
        };

        test.each([
            ['ctrl_stream', () => QueueNames.ctrl_stream(ids.agent_type), 'byai_gateway:v2:ctrl:agent_type:chat'],
            [
                'worker_ctrl_stream',
                () => QueueNames.worker_ctrl_stream(ids.worker_id),
                'byai_gateway:v2:ctrl:worker:{worker-01}',
            ],
            [
                'session_data_stream',
                () => QueueNames.session_data_stream(ids.session_id),
                'byai_gateway:v2:session:{sess-abc123}:data_stream',
            ],
            ['task_group', () => QueueNames.task_group(ids.group_id), 'byai_gateway:v2:task_group:{tg-1}'],
            [
                'task_group_results',
                () => QueueNames.task_group_results(ids.group_id),
                'byai_gateway:v2:task_group:{tg-1}:results',
            ],
            ['trace_meta', () => QueueNames.trace_meta(ids.trace_id), 'byai_gateway:v2:trace:{trace-xyz}'],
            ['trace_spans', () => QueueNames.trace_spans(ids.trace_id), 'byai_gateway:v2:trace:spans:{trace-xyz}'],
            [
                'trace_index_session',
                () => QueueNames.trace_index_session(ids.session_id),
                'byai_gateway:v2:trace:idx:session:sess-abc123',
            ],
            [
                'trace_index_worker',
                () => QueueNames.trace_index_worker(ids.worker_id),
                'byai_gateway:v2:trace:idx:worker:worker-01',
            ],
            [
                'trace_index_agent',
                () => QueueNames.trace_index_agent(ids.agent_type),
                'byai_gateway:v2:trace:idx:agent:chat',
            ],
            ['known_workers', () => RegistryKeys.known_workers(), 'byai_gateway:v2:registry:workers'],
            [
                'sd_active_instances',
                () => RegistryKeys.sd_active_instances(ids.service_name),
                'byai_gateway:v2:sd:{svc-a}:active',
            ],
            [
                'sd_instance_details',
                () => RegistryKeys.sd_instance_details(ids.service_name),
                'byai_gateway:v2:sd:{svc-a}:instances',
            ],
            ['sd_services', () => RegistryKeys.sd_services(), 'byai_gateway:v2:sd:services'],
            [
                'worker_online_lease',
                () => RegistryKeys.worker_online_lease(ids.worker_id),
                'byai_gateway:v2:registry:worker:{worker-01}:online',
            ],
            [
                'workerDeclaredAgentTypes',
                () => RegistryKeys.workerDeclaredAgentTypes(ids.worker_id),
                'byai_gateway:v2:registry:worker:{worker-01}:agent_types',
            ],
            [
                'agentTypeMembers',
                () => RegistryKeys.agentTypeMembers(ids.agent_type),
                'byai_gateway:v2:registry:agent_type:{chat}:workers',
            ],
            [
                'workerAdminState',
                () => RegistryKeys.workerAdminState(ids.worker_id),
                'byai_gateway:v2:registry:worker:{worker-01}:admin',
            ],
            [
                'agentTypeDenied',
                () => RegistryKeys.agentTypeDenied(ids.agent_type),
                'byai_gateway:v2:registry:agent_type:{chat}:denied',
            ],
            [
                'worker_lock',
                () => RegistryKeys.worker_lock(ids.worker_id),
                'byai_gateway:v2:registry:worker:{worker-01}:lock',
            ],
            [
                'session_registry',
                () => RegistryKeys.session_registry(ids.session_id),
                'byai_gateway:v2:session:{sess-abc123}:registry',
            ],
        ])('%s matches the golden v2 key', (_name, produce, expected) => {
            expect(produce()).toBe(expected);
        });
    });

    describe('v2 same-entity groups share one Cluster hash slot', () => {
        beforeEach(() => {
            process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';
        });

        function expectSameSlot(keys: string[]) {
            const slots = keys.map(slot);
            expect(new Set(slots).size).toBe(1);
        }

        test('worker group', () => {
            expectSameSlot([
                QueueNames.worker_ctrl_stream('worker-01'),
                RegistryKeys.worker_online_lease('worker-01'),
                RegistryKeys.workerDeclaredAgentTypes('worker-01'),
                RegistryKeys.workerAdminState('worker-01'),
                RegistryKeys.worker_lock('worker-01'),
            ]);
        });

        test('session group', () => {
            expectSameSlot([
                QueueNames.session_data_stream('sess-abc123'),
                RegistryKeys.session_registry('sess-abc123'),
            ]);
        });

        test('task_group group', () => {
            expectSameSlot([QueueNames.task_group('tg-1'), QueueNames.task_group_results('tg-1')]);
        });

        test('trace group', () => {
            expectSameSlot([QueueNames.trace_meta('trace-xyz'), QueueNames.trace_spans('trace-xyz')]);
        });

        test('agent_type group (mandatory tag)', () => {
            expectSameSlot([RegistryKeys.agentTypeMembers('chat'), RegistryKeys.agentTypeDenied('chat')]);
        });

        test('service-discovery group', () => {
            expectSameSlot([
                RegistryKeys.sd_active_instances('svc-a'),
                RegistryKeys.sd_instance_details('svc-a'),
            ]);
        });

        test('global-index keys carry no hash tag', () => {
            expect(RegistryKeys.known_workers()).not.toMatch(/[{}]/);
            expect(RegistryKeys.sd_services()).not.toMatch(/[{}]/);
        });

        test('cross-entity trace indexes carry no hash tag', () => {
            expect(QueueNames.trace_index_session('sess-abc123')).not.toMatch(/[{}]/);
            expect(QueueNames.trace_index_worker('worker-01')).not.toMatch(/[{}]/);
            expect(QueueNames.trace_index_agent('chat')).not.toMatch(/[{}]/);
        });
    });

    describe('worker_online_lease scan pattern helper', () => {
        test('v1: prefix pattern round-trips through startsWith slicing', () => {
            delete process.env.REDIS_KEY_SCHEMA_VERSION;
            const pattern = RegistryKeys.worker_online_lease_scan_pattern();
            expect(pattern).toBe('byai_gateway:registry:worker:online:*');
            const key = RegistryKeys.worker_online_lease('worker-07');
            expect(RegistryKeys.worker_id_from_online_lease_key(key)).toBe('worker-07');
        });

        test('v2: pattern accounts for the hash tag wrapping the worker_id mid-key', () => {
            process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';
            const pattern = RegistryKeys.worker_online_lease_scan_pattern();
            expect(pattern).toBe('byai_gateway:v2:registry:worker:{*}:online');
            const key = RegistryKeys.worker_online_lease('worker-07');
            expect(RegistryKeys.worker_id_from_online_lease_key(key)).toBe('worker-07');
        });
    });
});
