import * as path from 'path';

let activeWorkspace = '';

export function setActiveWorkspace(workspace: string): void {
    activeWorkspace = workspace;
}

export function getActiveWorkspace(): string {
    return activeWorkspace;
}

export class HookSandbox {
    install(): void {
        // Node 侧保持轻量实现：由业务在文件访问前调用 assertPathAllowed 进行校验。
    }

    uninstall(): void {}

    assertPathAllowed(filePath: string): void {
        if (!activeWorkspace) {
            return;
        }
        const absPath = path.resolve(filePath);
        const absWorkspace = path.resolve(activeWorkspace);
        if (!absPath.startsWith(absWorkspace)) {
            throw new Error(`[Sandbox] Access denied to path outside workspace: ${filePath}`);
        }
    }
}

