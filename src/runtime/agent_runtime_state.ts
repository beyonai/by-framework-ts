/**
 * Agent runtime state container.
 *
 * Provides a unified state container for agent execution including:
 * - SessionManager: Session and file management
 * - AgentConfigManager: Agent configuration management
 */

import { AgentConfig } from '../extensions/agent_config';
import { FileStorage } from './filestore';
import { AgentConfigManager } from './agent_config_manager';
import { SessionManager } from './session_manager';

export class AgentRuntimeState {
  private readonly sessionManager: SessionManager;
  private readonly configManager: AgentConfigManager;

  constructor(options: {
    sessionId: string;
    userCode?: string;
    userName?: string;
    storage?: FileStorage;
    workspaceDir?: string;
    agentConfigs?: AgentConfig[];
  }) {
    this.sessionManager = new SessionManager(
      options.sessionId,
      options.userCode,
      options.userName,
      options.storage,
      options.workspaceDir
    );
    this.configManager = new AgentConfigManager(options.agentConfigs);
  }

  get session_manager(): SessionManager {
    return this.sessionManager;
  }

  get config_manager(): AgentConfigManager {
    return this.configManager;
  }
}
