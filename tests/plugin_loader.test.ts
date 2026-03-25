import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { PluginRegistry } from '../src/extensions/registry';

describe('PluginRegistry.loadPluginsFromDir', () => {
    test('loads plugin class from filesystem module and registers agent config', async () => {
        const tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'gateway-ts-plugin-'));
        const pluginFile = path.join(tmpRoot, 'file_plugin.cjs');
        const pluginModulePath = path.resolve(__dirname, '../src/extensions/plugin');

        const pluginCode = `
const { Plugin } = require(${JSON.stringify(pluginModulePath)});

class FilePlugin extends Plugin {
  constructor() {
    super({ plugin_id: 'file_plugin', priority: 1, enabled: true });
  }

  async registerAgentConfigs(buildContext) {
    const configs = buildContext.listAgentConfigs();
    return [
      ...configs,
      {
        agent_id: 'file_agent',
        name: 'File Agent',
        tools: { ping: () => 'pong' },
        on_conflict: 'overwrite'
      }
    ];
  }
}

module.exports = { FilePlugin };
`;
        await fs.writeFile(pluginFile, pluginCode, 'utf-8');

        const registry = new PluginRegistry();
        await registry.loadPluginsFromDir(tmpRoot);
        await registry.initializePlugins();

        expect(registry.getPlugin('file_plugin')).toBeDefined();
        const config = registry.agentConfigs.find((c) => c.agent_id === 'file_agent');
        expect(config).toBeDefined();
        expect(config?.name).toBe('File Agent');
        expect(typeof config?.tools?.ping).toBe('function');

        await fs.rm(tmpRoot, { recursive: true, force: true });
    });

    test('ignores invalid directory without throwing', async () => {
        const registry = new PluginRegistry();
        await expect(registry.loadPluginsFromDir('/path/does/not/exist')).resolves.toBeUndefined();
    });
});
