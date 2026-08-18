// ignore: unused_import
import 'package:intl/intl.dart' as intl;
import 'l10n.dart';

// ignore_for_file: type=lint

/// The translations for Chinese (`zh`).
class AppLocalizationsZh extends AppLocalizations {
  AppLocalizationsZh([String locale = 'zh']) : super(locale);

  @override
  String get title => 'AgentTalk';

  @override
  String get selectProject => '选择项目';

  @override
  String get selectConversation => '选择对话';

  @override
  String get noProjectOrConversation => '请先选择项目或对话';

  @override
  String get storeMemorySuccess => '记忆存储成功';

  @override
  String get errorInvalidProject => '无效的项目';

  @override
  String get errorInvalidConversation => '无效的对话';

  @override
  String get pleaseSelectProjectOrConversation => '请先选择项目或对话';

  @override
  String get pleaseSelectProject => '请先选择项目';

  @override
  String get pleaseSelectConversationAddAttachment => '请先选择对话后再添加附件';

  @override
  String get cancel => '取消';

  @override
  String get save => '保存';

  @override
  String get confirm => '确认';

  @override
  String get createAgent => '新建智能体';

  @override
  String get editAgent => '编辑智能体';

  @override
  String get connectorCenter => 'Connector 中心';

  @override
  String get connectorDiscovery => '连接器发现';

  @override
  String get localDiscovery => '本地发现';

  @override
  String get addAgent => '添加智能体';

  @override
  String get scanLocalAgents => '扫描本地智能体';

  @override
  String get manualAddAgent => '手动添加';

  @override
  String get contextInspector => '上下文检视器';

  @override
  String get eventRecovery => '事件流恢复';

  @override
  String get diagnostics => '诊断与元数据';

  @override
  String get searchMessages => '搜索消息';

  @override
  String get writeMemory => '写入记忆';

  @override
  String get projectAgents => '项目智能体';

  @override
  String get projectionEntity => '投影实体';

  @override
  String get retrievalSource => '写入检索源';

  @override
  String get retrievalSelection => '检索选择';

  @override
  String get retrievalPreview => '检索预览';

  @override
  String get createWorkflow => '创建工作流';

  @override
  String get setAsDefaultModelSuccess => '已设为默认模型: ';

  @override
  String get setAsDefaultModelFailed => '设置默认失败: ';

  @override
  String get refresh => '刷新';

  @override
  String get catalogUnavailableOrLoadFailed => '目录不可用/加载失败: ';

  @override
  String get availableModelsFromCore => '可用模型（来自 Core）:';

  @override
  String get sourceLabel => '来源: ';

  @override
  String get availabilityLabel => '可用性: ';

  @override
  String get setAsDefault => '设为默认';

  @override
  String get allFieldsCannotBeEmpty => '所有字段（包含 Connector ID 与模型 ID）均不能为空';

  @override
  String get displayNameLabel => '显示名称 (Name)';

  @override
  String get displayNameHint => '例如 架构师 / Codex';

  @override
  String get roleLabel => '角色 (Role)';

  @override
  String get roleHint => '例如 全栈工程师 / 架构评估';

  @override
  String get specialtyLabel => '专长 (Specialty)';

  @override
  String get specialtyHint => '例如 Flutter / Rust / 性能优化';

  @override
  String get systemPromptLabel => '系统提示词 (System Prompt)';

  @override
  String get manuallySpecifiedUnverified => '已手动指定 (未验证)';

  @override
  String get scanLocalAgentsEmptyTitle => '当前还没有本地智能体';

  @override
  String get scanLocalAgentsEmptySubtitle => '你可以先扫描本地智能体，或确认候选后手动添加。';

  @override
  String get scanLocalAgentsScanning => '正在扫描本地智能体…';

  @override
  String get scanLocalAgentsNoResults => '没有发现本地智能体。';

  @override
  String get scanLocalAgentsPartial => '部分候选需要配置或认证。';

  @override
  String get scanLocalAgentsRequiresConfig => '需要配置';

  @override
  String get scanLocalAgentsRequiresAuth => '需要认证';

  @override
  String get scanLocalAgentsFailed => '本地智能体扫描失败：';

  @override
  String get scanLocalAgentsRetry => '重试';

  @override
  String get scanLocalAgentsRescan => '重新扫描';

  @override
  String get scanLocalAgentsUseCandidate => '使用此候选';

  @override
  String get scanLocalAgentsManualFallback => '手动添加';

  @override
  String get discoveryConnectorIdLabel => 'connectorId';

  @override
  String get discoveryRuntimeTypeLabel => 'runtimeType';

  @override
  String get discoveryDisplayNameLabel => 'displayName';

  @override
  String get discoveryAvailabilityLabel => 'availability';

  @override
  String get discoveryModelsLabel => 'models';

  @override
  String get discoveryCatalogRevisionLabel => 'catalogRevision';

  @override
  String get discoverySourceLabel => 'source';

  @override
  String get discoveryRequiresConfigurationLabel => 'requiresConfiguration';

  @override
  String get availabilityAvailable => '可用';

  @override
  String get availabilityUnavailable => '不可用';

  @override
  String get availabilityUnconfigured => '需要配置';

  @override
  String get availabilityAuthenticationRequired => '需要认证';

  @override
  String get availabilityPartial => '部分可用';

  @override
  String get availabilityUnknown => '未知';

  @override
  String get localAgentScanDialogTitle => '扫描与导入本地智能体';

  @override
  String get localAgentScanDialogDescription =>
      '被动扫描本机候选并按类别分组展示；验证仅执行受控的协议握手（initialize），导入为原子操作。';

  @override
  String get localAgentRescan => '重新扫描';

  @override
  String get localAgentManualAdd => '手动添加';

  @override
  String get localAgentSelectExecutable => '选择文件并验证';

  @override
  String get localAgentScanning => '正在扫描…';

  @override
  String get localAgentNoCandidates => '没有发现本地候选。';

  @override
  String get localAgentCategoryAgent => '智能体';

  @override
  String get localAgentCategoryModelRuntime => '模型服务';

  @override
  String get localAgentCategoryToolServer => '工具服务';

  @override
  String get localAgentCategoryUnknown => '未知';

  @override
  String get localAgentGroupEmpty => '（无候选）';

  @override
  String get localAgentErrorShuttingDown => '服务正在关闭，请稍后重试。';

  @override
  String get localAgentErrorIdentityChanged => '候选身份已变化，请重新扫描后再试。';

  @override
  String get localAgentErrorConflict => '导入与已有记录冲突，无法继续。';

  @override
  String get localAgentErrorPersistence => '持久化失败，未能完成导入。';

  @override
  String get localAgentErrorCapacity => '当前容量已满，请稍后重试。';

  @override
  String get localAgentErrorScanMissing => '扫描不存在或已过期，请重新扫描。';

  @override
  String get localAgentErrorCandidateMissing => '候选不存在，请重新扫描。';

  @override
  String get localAgentErrorCandidateDismissed => '该候选已被隐藏。';

  @override
  String get localAgentErrorConsentRequired => '需要先确认验证同意。';

  @override
  String get localAgentErrorVerificationInProgress => '该候选正在验证中。';

  @override
  String get localAgentErrorAdapterRequired => '该候选需要适配器。';

  @override
  String get localAgentErrorScanWorkerUnavailable => '扫描服务暂不可用，请重试。';

  @override
  String get localAgentErrorPlanMismatch => '导入计划与当前选择不一致，请重新获取计划。';

  @override
  String get localAgentErrorGeneric => '操作失败，请重试。';

  @override
  String get localAgentStatusDiscovery => '发现';

  @override
  String get localAgentStatusCompatibility => '协议兼容';

  @override
  String get localAgentStatusAuth => '认证';

  @override
  String get localAgentStatusHealth => '健康';

  @override
  String get localAgentDiscoveryObserved => '已观察到';

  @override
  String get localAgentDiscoveryIdentified => '已识别';

  @override
  String get localAgentDiscoveryDisappeared => '已消失';

  @override
  String get localAgentCompatibilityCompatible => '兼容';

  @override
  String get localAgentCompatibilityIncompatible => '不兼容';

  @override
  String get localAgentCompatibilityAdapterRequired => '需要适配器';

  @override
  String get localAgentCompatibilityNotVerified => '未验证';

  @override
  String get localAgentAuthUnknown => '未知';

  @override
  String get localAgentAuthNotRequired => '不需要';

  @override
  String get localAgentAuthRequired => '需要登录';

  @override
  String get localAgentAuthReady => '就绪';

  @override
  String get localAgentHealthNotChecked => '未检查';

  @override
  String get localAgentHealthReady => '正常';

  @override
  String get localAgentHealthUnavailable => '不可用';

  @override
  String get localAgentHealthIdentityMismatch => '身份不匹配';

  @override
  String get localAgentLifecycleObserved => '已观察到';

  @override
  String get localAgentLifecycleIdentified => '已识别，等待验证';

  @override
  String get localAgentLifecycleVerifying => '正在验证…';

  @override
  String get localAgentLifecycleVerified => '已验证';

  @override
  String get localAgentLifecycleAuthRequired => '需要认证';

  @override
  String get localAgentLifecycleIdentityChanged => '身份已变化，请重新扫描';

  @override
  String get localAgentLifecycleNotVerified => '尚未验证';

  @override
  String get localAgentVerifyConsentTitle => '验证兼容性';

  @override
  String get localAgentVerifyConsentBody =>
      '验证将启动一次受控的协议握手，仅执行 initialize，不发送任何任务、提示或工具调用；验证进程受 Core 隔离并限时。';

  @override
  String get localAgentVerifyConsentAgree => '同意并验证';

  @override
  String get localAgentVerify => '验证兼容性';

  @override
  String get localAgentImport => '导入';

  @override
  String get localAgentDismiss => '隐藏';

  @override
  String get localAgentUnknownNeedsAdapter => '此候选需要选择适配器或清单后才能使用。';

  @override
  String get localAgentModelRuntimeNote => '模型服务：此类别需要单独的模型连接器流程（尚未提供）。';

  @override
  String get localAgentToolServerNote => '工具服务：此类别应进入工具中心（尚未提供）。';

  @override
  String get localAgentImportReusedNotice => '该智能体已导入过，本次复用已有记录。';

  @override
  String get localAgentEventReplayGapNotice => '事件流出现缺口，已回退到快照刷新。';

  @override
  String get localAgentEventStreamNotice => '事件订阅不可用，已改用快照刷新。';

  @override
  String get localAgentProjectRequired => '请先选择项目，再导入智能体。';

  @override
  String get localAgentImportDialogTitle => '导入智能体';

  @override
  String localAgentImportTargetProject(String projectId) {
    return '目标项目：$projectId';
  }

  @override
  String get localAgentModelSelectionTitle => '模型选择';

  @override
  String get localAgentModelConnectorDefault => '使用连接器默认模型（无需模型 ID）';

  @override
  String get localAgentModelConnectorDefaultHint =>
      'connector_default；不指定模型 ID 是合法的导入选项。';

  @override
  String get localAgentModelPinned => '指定模型';

  @override
  String get localAgentModelPinnedLabel => '模型';

  @override
  String get localAgentModelPinnedUnavailable => '此候选没有可用的模型列表，请使用连接器默认模型。';

  @override
  String get localAgentImportPlanLoading => '正在生成只读导入计划…';

  @override
  String get localAgentImportPlanMissing => '导入计划尚不可用。';

  @override
  String get localAgentImportPlanSummary => '导入计划摘要';

  @override
  String get localAgentImportPlanReadOnly => '只读计划';

  @override
  String get localAgentImportPlanConnector => '连接器';

  @override
  String get localAgentImportPlanAdapter => '适配器';

  @override
  String get localAgentImportPlanProtocol => '协议版本';

  @override
  String get localAgentImportPlanAuth => '认证';

  @override
  String get localAgentImportPlanAuthRequired => '需要认证';

  @override
  String get localAgentImportPlanModel => '模型';

  @override
  String get localAgentImportPlanActions => '计划操作：';

  @override
  String get localAgentImportConfirm => '确认导入';

  @override
  String get localAgentImportDone => '完成';

  @override
  String get localAgentImportSuccess => '导入成功';

  @override
  String get localAgentImportSuccessReused => '已导入（复用已有记录）';

  @override
  String localAgentImportReceiptNote(String agentId, String connectorId) {
    return '已创建智能体 $agentId，连接器 $connectorId。成功导入不等于已完成真实智能体调用。';
  }

  @override
  String get localAgentEvidenceExecutableInventory => '已安装可执行文件';

  @override
  String get localAgentEvidenceWindowsPath => '位于 PATH';

  @override
  String get localAgentEvidenceAppPaths => '注册于应用路径';

  @override
  String get localAgentEvidencePackage => '已安装软件包';

  @override
  String get localAgentEvidenceLoopback => '本地回环服务';

  @override
  String get localAgentEvidenceUserSelected => '用户选择';

  @override
  String get localAgentEvidenceRuntimeRecord => '有运行记录';

  @override
  String get localAgentEvidenceVersionMatched => '版本匹配';

  @override
  String get localAgentEvidenceBuildMatched => '构建匹配';

  @override
  String get localAgentEvidenceInstallKnown => '已知安装';

  @override
  String get localAgentEvidenceAvailable => '可用';

  @override
  String get localAgentEvidenceAuthRequired => '需要认证';

  @override
  String get localAgentEvidenceUnconfigured => '需要配置';

  @override
  String get localAgentEvidenceIdentityMismatch => '身份不匹配';

  @override
  String get localAgentEvidenceCatalogUnavailable => '目录不可用';

  @override
  String get connectorDiscoverEmptyTitle => '当前还没有发现连接器';

  @override
  String get connectorDiscoverEmptySubtitle =>
      '刷新后会真实调用 connector.discover 来查看本地候选。';

  @override
  String get connectorDiscoverScannning => '正在发现连接器…';

  @override
  String get connectorDiscoverFailed => '连接器发现失败：';

  @override
  String get connectorDiscoverRetry => '重试';

  @override
  String get connectorDiscoverRescan => '刷新';

  @override
  String get connectorDiscoverNotFound => '没有发现本地连接器。';

  @override
  String get connectorDiscoverSupported => '已发现本地连接器候选';

  @override
  String get connectorDiscoverManualFallback => '管理配置';

  @override
  String get advancedDiagnosticsTitle => '高级诊断';

  @override
  String get advancedDiagnosticsSubtitle => '运行状态与投影元数据';

  @override
  String get technicalDiagnosticsDetails => '技术诊断详情';

  @override
  String get retryStartup => '重试启动';

  @override
  String get coreHealth => '核心健康状态';

  @override
  String get coreProjectionReady => '核心投影已就绪';

  @override
  String get coreProjectionUnavailable => '核心投影不可用';

  @override
  String get coreEventStreamError => '核心事件流错误：';

  @override
  String get coreProjectionReconnected => '核心投影已重新连接';

  @override
  String get coreEventStreamStopped => '事件订阅失败，应用已停止继续应用事件。';

  @override
  String get coreEventRecoveryFailed => '事件恢复失败，仍保持 fail-closed。';

  @override
  String get projectHasNoAgents => '当前项目还没有智能体。';

  @override
  String get projectAgentEmptyHint => '扫描或手动添加后，智能体会出现在这里。';

  @override
  String get scanLocalAgentsTitle => '扫描本地智能体';

  @override
  String get scanLocalAgentsDescription =>
      '此操作会真实调用 agent.scan_local，不会自动创建身份。';

  @override
  String get searchMessagesHint => '搜索当前对话历史消息';

  @override
  String get searchMessagesEmpty => '输入关键词搜索消息';

  @override
  String get searchMessagesFailed => '搜索失败：';

  @override
  String get composerTools => '编写器工具';

  @override
  String get send => '发送';

  @override
  String get stopActiveRun => '停止当前运行';

  @override
  String get attachment => '附件';

  @override
  String get memory => 'Memory';

  @override
  String get saveMemorySource => '保存 Memory';

  @override
  String get retrieval => 'Retrieval';

  @override
  String get saveRetrievalSource => '保存 Retrieval 源';

  @override
  String get agentPicker => '选择智能体';

  @override
  String get agentPanel => '智能体面板';

  @override
  String get workflowPanel => '工作流面板';

  @override
  String get toggleTheme => '切换主题';

  @override
  String get project => '项目';

  @override
  String get conversation => '对话';
}
