import { HookSandbox, setActiveWorkspace, getActiveWorkspace } from '../src/sandbox';

/**
 * Sandbox 测试，对标 Python test_sandbox.py
 */
describe('HookSandbox', () => {
    const sandbox = new HookSandbox();

    afterEach(() => {
        setActiveWorkspace('');
    });

    test('allows access within workspace directory', () => {
        setActiveWorkspace('/tmp/workspace/sess-1/private/task-1');

        expect(() => {
            sandbox.assertPathAllowed('/tmp/workspace/sess-1/private/task-1/output/result.json');
        }).not.toThrow();
    });

    test('denies access outside workspace directory', () => {
        setActiveWorkspace('/tmp/workspace/sess-1/private/task-1');

        expect(() => {
            sandbox.assertPathAllowed('/etc/passwd');
        }).toThrow('Access denied');
    });

    test('allows any path when no workspace is active', () => {
        setActiveWorkspace('');

        expect(() => {
            sandbox.assertPathAllowed('/any/path/file.txt');
        }).not.toThrow();
    });

    test('setActiveWorkspace and getActiveWorkspace round trip', () => {
        setActiveWorkspace('/tmp/test');
        expect(getActiveWorkspace()).toBe('/tmp/test');

        setActiveWorkspace('');
        expect(getActiveWorkspace()).toBe('');
    });

    test('install and uninstall are safe no-ops', () => {
        expect(() => {
            sandbox.install();
            sandbox.uninstall();
        }).not.toThrow();
    });
});
