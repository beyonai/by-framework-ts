import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { WorkspaceManager } from '../src/workspace';

/**
 * Workspace 独立测试，对标 Python test_workspace.py
 */
describe('WorkspaceManager', () => {
    let tmpDir: string;
    let manager: WorkspaceManager;

    beforeEach(async () => {
        tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'ws-test-'));
        manager = new WorkspaceManager(tmpDir);
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('setupWorkspace creates expected directory structure', async () => {
        const paths = await manager.setupWorkspace('sess-1', 'task-1');

        expect(paths.root).toBe(path.join(tmpDir, 'sess-1'));
        expect(paths.public).toBe(path.join(tmpDir, 'sess-1', 'public'));
        expect(paths.private).toBe(path.join(tmpDir, 'sess-1', 'private', 'task-1'));

        // Public subdirectories
        const publicDirs = ['session', 'memory/local_db', 'agent_skills'];
        for (const sub of publicDirs) {
            const stat = await fs.stat(path.join(paths.public, sub));
            expect(stat.isDirectory()).toBe(true);
        }

        // Private subdirectories
        const privateDirs = ['input', 'temp', 'output', 'system'];
        for (const sub of privateDirs) {
            const stat = await fs.stat(path.join(paths.private, sub));
            expect(stat.isDirectory()).toBe(true);
        }
    });

    test('history_db path is under public/session', async () => {
        const paths = await manager.setupWorkspace('sess-2', 'task-2');
        expect(paths.history_db).toBe(path.join(paths.public, 'session', 'history.json'));
    });

    test('cleanupTask removes private task directory', async () => {
        const paths = await manager.setupWorkspace('sess-3', 'task-3');

        // Verify private dir exists
        const statBefore = await fs.stat(paths.private);
        expect(statBefore.isDirectory()).toBe(true);

        await manager.cleanupTask('sess-3', 'task-3');

        // Private task dir should be gone
        await expect(fs.stat(paths.private)).rejects.toThrow();
    });

    test('cleanupTask preserves public directory', async () => {
        const paths = await manager.setupWorkspace('sess-4', 'task-4');

        await manager.cleanupTask('sess-4', 'task-4');

        // Public dir should still exist
        const stat = await fs.stat(paths.public);
        expect(stat.isDirectory()).toBe(true);
    });

    test('cleanupTask is safe for nonexistent task', async () => {
        await expect(manager.cleanupTask('nonexistent', 'no-task')).resolves.not.toThrow();
    });
});
