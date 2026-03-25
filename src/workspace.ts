import * as fs from 'fs/promises';
import * as path from 'path';

export interface WorkspacePaths {
    root: string;
    public: string;
    private: string;
    history_db: string;
}

export class WorkspaceManager {
    constructor(private readonly baseDir: string = '/tmp/workspace') {}

    async setupWorkspace(sessionId: string, taskId: string): Promise<WorkspacePaths> {
        const sessionDir = path.join(this.baseDir, sessionId);
        const publicDir = path.join(sessionDir, 'public');
        const privateTaskDir = path.join(sessionDir, 'private', taskId);

        for (const subDir of ['session', path.join('memory', 'local_db'), 'agent_skills']) {
            await fs.mkdir(path.join(publicDir, subDir), { recursive: true });
        }
        for (const subDir of ['input', 'temp', 'output', 'system']) {
            await fs.mkdir(path.join(privateTaskDir, subDir), { recursive: true });
        }

        return {
            root: sessionDir,
            public: publicDir,
            private: privateTaskDir,
            history_db: path.join(publicDir, 'session', 'history.json'),
        };
    }

    async cleanupTask(sessionId: string, taskId: string): Promise<void> {
        const taskDir = path.join(this.baseDir, sessionId, 'private', taskId);
        await fs.rm(taskDir, { recursive: true, force: true });
    }
}

