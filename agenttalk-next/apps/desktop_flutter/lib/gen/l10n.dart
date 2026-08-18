import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_localizations/flutter_localizations.dart';
import 'package:intl/intl.dart' as intl;

import 'l10n_en.dart';
import 'l10n_zh.dart';

// ignore_for_file: type=lint

/// Callers can lookup localized strings with an instance of AppLocalizations
/// returned by `AppLocalizations.of(context)`.
///
/// Applications need to include `AppLocalizations.delegate()` in their app's
/// `localizationDelegates` list, and the locales they support in the app's
/// `supportedLocales` list. For example:
///
/// ```dart
/// import 'gen/l10n.dart';
///
/// return MaterialApp(
///   localizationsDelegates: AppLocalizations.localizationsDelegates,
///   supportedLocales: AppLocalizations.supportedLocales,
///   home: MyApplicationHome(),
/// );
/// ```
///
/// ## Update pubspec.yaml
///
/// Please make sure to update your pubspec.yaml to include the following
/// packages:
///
/// ```yaml
/// dependencies:
///   # Internationalization support.
///   flutter_localizations:
///     sdk: flutter
///   intl: any # Use the pinned version from flutter_localizations
///
///   # Rest of dependencies
/// ```
///
/// ## iOS Applications
///
/// iOS applications define key application metadata, including supported
/// locales, in an Info.plist file that is built into the application bundle.
/// To configure the locales supported by your app, you’ll need to edit this
/// file.
///
/// First, open your project’s ios/Runner.xcworkspace Xcode workspace file.
/// Then, in the Project Navigator, open the Info.plist file under the Runner
/// project’s Runner folder.
///
/// Next, select the Information Property List item, select Add Item from the
/// Editor menu, then select Localizations from the pop-up menu.
///
/// Select and expand the newly-created Localizations item then, for each
/// locale your application supports, add a new item and select the locale
/// you wish to add from the pop-up menu in the Value field. This list should
/// be consistent with the languages listed in the AppLocalizations.supportedLocales
/// property.
abstract class AppLocalizations {
  AppLocalizations(String locale)
    : localeName = intl.Intl.canonicalizedLocale(locale.toString());

  final String localeName;

  static AppLocalizations? of(BuildContext context) {
    return Localizations.of<AppLocalizations>(context, AppLocalizations);
  }

  static const LocalizationsDelegate<AppLocalizations> delegate =
      _AppLocalizationsDelegate();

  /// A list of this localizations delegate along with the default localizations
  /// delegates.
  ///
  /// Returns a list of localizations delegates containing this delegate along with
  /// GlobalMaterialLocalizations.delegate, GlobalCupertinoLocalizations.delegate,
  /// and GlobalWidgetsLocalizations.delegate.
  ///
  /// Additional delegates can be added by appending to this list in
  /// MaterialApp. This list does not have to be used at all if a custom list
  /// of delegates is preferred or required.
  static const List<LocalizationsDelegate<dynamic>> localizationsDelegates =
      <LocalizationsDelegate<dynamic>>[
        delegate,
        GlobalMaterialLocalizations.delegate,
        GlobalCupertinoLocalizations.delegate,
        GlobalWidgetsLocalizations.delegate,
      ];

  /// A list of this localizations delegate's supported locales.
  static const List<Locale> supportedLocales = <Locale>[
    Locale('en'),
    Locale('zh'),
  ];

  /// No description provided for @title.
  ///
  /// In en, this message translates to:
  /// **'AgentTalk'**
  String get title;

  /// No description provided for @selectProject.
  ///
  /// In en, this message translates to:
  /// **'Select Project'**
  String get selectProject;

  /// No description provided for @selectConversation.
  ///
  /// In en, this message translates to:
  /// **'Select Conversation'**
  String get selectConversation;

  /// No description provided for @noProjectOrConversation.
  ///
  /// In en, this message translates to:
  /// **'Please select a Project or Conversation first'**
  String get noProjectOrConversation;

  /// No description provided for @storeMemorySuccess.
  ///
  /// In en, this message translates to:
  /// **'Memory stored successfully'**
  String get storeMemorySuccess;

  /// No description provided for @errorInvalidProject.
  ///
  /// In en, this message translates to:
  /// **'Invalid Project'**
  String get errorInvalidProject;

  /// No description provided for @errorInvalidConversation.
  ///
  /// In en, this message translates to:
  /// **'Invalid Conversation'**
  String get errorInvalidConversation;

  /// No description provided for @pleaseSelectProjectOrConversation.
  ///
  /// In en, this message translates to:
  /// **'Please select a Project or Conversation first'**
  String get pleaseSelectProjectOrConversation;

  /// No description provided for @pleaseSelectProject.
  ///
  /// In en, this message translates to:
  /// **'Please select a Project first'**
  String get pleaseSelectProject;

  /// No description provided for @pleaseSelectConversationAddAttachment.
  ///
  /// In en, this message translates to:
  /// **'Please select a Conversation first before adding attachment'**
  String get pleaseSelectConversationAddAttachment;

  /// No description provided for @cancel.
  ///
  /// In en, this message translates to:
  /// **'Cancel'**
  String get cancel;

  /// No description provided for @save.
  ///
  /// In en, this message translates to:
  /// **'Save'**
  String get save;

  /// No description provided for @confirm.
  ///
  /// In en, this message translates to:
  /// **'Confirm'**
  String get confirm;

  /// No description provided for @createAgent.
  ///
  /// In en, this message translates to:
  /// **'New Agent'**
  String get createAgent;

  /// No description provided for @editAgent.
  ///
  /// In en, this message translates to:
  /// **'Edit Agent'**
  String get editAgent;

  /// No description provided for @connectorCenter.
  ///
  /// In en, this message translates to:
  /// **'Connector Center'**
  String get connectorCenter;

  /// No description provided for @connectorDiscovery.
  ///
  /// In en, this message translates to:
  /// **'Connector discovery'**
  String get connectorDiscovery;

  /// No description provided for @localDiscovery.
  ///
  /// In en, this message translates to:
  /// **'Local discovery'**
  String get localDiscovery;

  /// No description provided for @addAgent.
  ///
  /// In en, this message translates to:
  /// **'Add Agent'**
  String get addAgent;

  /// No description provided for @scanLocalAgents.
  ///
  /// In en, this message translates to:
  /// **'Scan local agents'**
  String get scanLocalAgents;

  /// No description provided for @manualAddAgent.
  ///
  /// In en, this message translates to:
  /// **'Add manually'**
  String get manualAddAgent;

  /// No description provided for @contextInspector.
  ///
  /// In en, this message translates to:
  /// **'Context Inspector'**
  String get contextInspector;

  /// No description provided for @eventRecovery.
  ///
  /// In en, this message translates to:
  /// **'Event Stream Recovery'**
  String get eventRecovery;

  /// No description provided for @diagnostics.
  ///
  /// In en, this message translates to:
  /// **'Diagnostics & Metadata'**
  String get diagnostics;

  /// No description provided for @searchMessages.
  ///
  /// In en, this message translates to:
  /// **'Search Messages'**
  String get searchMessages;

  /// No description provided for @writeMemory.
  ///
  /// In en, this message translates to:
  /// **'Write Memory'**
  String get writeMemory;

  /// No description provided for @projectAgents.
  ///
  /// In en, this message translates to:
  /// **'Project Agent Roster'**
  String get projectAgents;

  /// No description provided for @projectionEntity.
  ///
  /// In en, this message translates to:
  /// **'Projection Entity'**
  String get projectionEntity;

  /// No description provided for @retrievalSource.
  ///
  /// In en, this message translates to:
  /// **'Write Retrieval Source'**
  String get retrievalSource;

  /// No description provided for @retrievalSelection.
  ///
  /// In en, this message translates to:
  /// **'Retrieval Selection'**
  String get retrievalSelection;

  /// No description provided for @retrievalPreview.
  ///
  /// In en, this message translates to:
  /// **'Retrieval Preview'**
  String get retrievalPreview;

  /// No description provided for @createWorkflow.
  ///
  /// In en, this message translates to:
  /// **'Create Workflow'**
  String get createWorkflow;

  /// No description provided for @setAsDefaultModelSuccess.
  ///
  /// In en, this message translates to:
  /// **'Set as default model: '**
  String get setAsDefaultModelSuccess;

  /// No description provided for @setAsDefaultModelFailed.
  ///
  /// In en, this message translates to:
  /// **'Failed to set default: '**
  String get setAsDefaultModelFailed;

  /// No description provided for @refresh.
  ///
  /// In en, this message translates to:
  /// **'Refresh'**
  String get refresh;

  /// No description provided for @catalogUnavailableOrLoadFailed.
  ///
  /// In en, this message translates to:
  /// **'Catalog unavailable/load failed: '**
  String get catalogUnavailableOrLoadFailed;

  /// No description provided for @availableModelsFromCore.
  ///
  /// In en, this message translates to:
  /// **'Available models (from Core):'**
  String get availableModelsFromCore;

  /// No description provided for @sourceLabel.
  ///
  /// In en, this message translates to:
  /// **'Source: '**
  String get sourceLabel;

  /// No description provided for @availabilityLabel.
  ///
  /// In en, this message translates to:
  /// **'Availability: '**
  String get availabilityLabel;

  /// No description provided for @setAsDefault.
  ///
  /// In en, this message translates to:
  /// **'Set Default'**
  String get setAsDefault;

  /// No description provided for @allFieldsCannotBeEmpty.
  ///
  /// In en, this message translates to:
  /// **'All fields (including Connector ID and Model ID) cannot be empty'**
  String get allFieldsCannotBeEmpty;

  /// No description provided for @displayNameLabel.
  ///
  /// In en, this message translates to:
  /// **'Display Name (Name)'**
  String get displayNameLabel;

  /// No description provided for @displayNameHint.
  ///
  /// In en, this message translates to:
  /// **'e.g. Architect / Codex'**
  String get displayNameHint;

  /// No description provided for @roleLabel.
  ///
  /// In en, this message translates to:
  /// **'Role'**
  String get roleLabel;

  /// No description provided for @roleHint.
  ///
  /// In en, this message translates to:
  /// **'e.g. Full-stack Engineer / Architecture Evaluation'**
  String get roleHint;

  /// No description provided for @specialtyLabel.
  ///
  /// In en, this message translates to:
  /// **'Specialty'**
  String get specialtyLabel;

  /// No description provided for @specialtyHint.
  ///
  /// In en, this message translates to:
  /// **'e.g. Flutter / Rust / Performance Optimization'**
  String get specialtyHint;

  /// No description provided for @systemPromptLabel.
  ///
  /// In en, this message translates to:
  /// **'System Prompt'**
  String get systemPromptLabel;

  /// No description provided for @manuallySpecifiedUnverified.
  ///
  /// In en, this message translates to:
  /// **'Manually specified (unverified)'**
  String get manuallySpecifiedUnverified;

  /// No description provided for @scanLocalAgentsEmptyTitle.
  ///
  /// In en, this message translates to:
  /// **'No local agents yet'**
  String get scanLocalAgentsEmptyTitle;

  /// No description provided for @scanLocalAgentsEmptySubtitle.
  ///
  /// In en, this message translates to:
  /// **'Scan local agents or add one manually after you confirm a candidate.'**
  String get scanLocalAgentsEmptySubtitle;

  /// No description provided for @scanLocalAgentsScanning.
  ///
  /// In en, this message translates to:
  /// **'Scanning local agents…'**
  String get scanLocalAgentsScanning;

  /// No description provided for @scanLocalAgentsNoResults.
  ///
  /// In en, this message translates to:
  /// **'No local agents were found.'**
  String get scanLocalAgentsNoResults;

  /// No description provided for @scanLocalAgentsPartial.
  ///
  /// In en, this message translates to:
  /// **'Some candidates need configuration or authentication.'**
  String get scanLocalAgentsPartial;

  /// No description provided for @scanLocalAgentsRequiresConfig.
  ///
  /// In en, this message translates to:
  /// **'Needs configuration'**
  String get scanLocalAgentsRequiresConfig;

  /// No description provided for @scanLocalAgentsRequiresAuth.
  ///
  /// In en, this message translates to:
  /// **'Needs authentication'**
  String get scanLocalAgentsRequiresAuth;

  /// No description provided for @scanLocalAgentsFailed.
  ///
  /// In en, this message translates to:
  /// **'Local agent scan failed: '**
  String get scanLocalAgentsFailed;

  /// No description provided for @scanLocalAgentsRetry.
  ///
  /// In en, this message translates to:
  /// **'Retry'**
  String get scanLocalAgentsRetry;

  /// No description provided for @scanLocalAgentsRescan.
  ///
  /// In en, this message translates to:
  /// **'Rescan'**
  String get scanLocalAgentsRescan;

  /// No description provided for @scanLocalAgentsUseCandidate.
  ///
  /// In en, this message translates to:
  /// **'Use this candidate'**
  String get scanLocalAgentsUseCandidate;

  /// No description provided for @scanLocalAgentsManualFallback.
  ///
  /// In en, this message translates to:
  /// **'Add manually'**
  String get scanLocalAgentsManualFallback;

  /// No description provided for @discoveryConnectorIdLabel.
  ///
  /// In en, this message translates to:
  /// **'connectorId'**
  String get discoveryConnectorIdLabel;

  /// No description provided for @discoveryRuntimeTypeLabel.
  ///
  /// In en, this message translates to:
  /// **'runtimeType'**
  String get discoveryRuntimeTypeLabel;

  /// No description provided for @discoveryDisplayNameLabel.
  ///
  /// In en, this message translates to:
  /// **'displayName'**
  String get discoveryDisplayNameLabel;

  /// No description provided for @discoveryAvailabilityLabel.
  ///
  /// In en, this message translates to:
  /// **'availability'**
  String get discoveryAvailabilityLabel;

  /// No description provided for @discoveryModelsLabel.
  ///
  /// In en, this message translates to:
  /// **'models'**
  String get discoveryModelsLabel;

  /// No description provided for @discoveryCatalogRevisionLabel.
  ///
  /// In en, this message translates to:
  /// **'catalogRevision'**
  String get discoveryCatalogRevisionLabel;

  /// No description provided for @discoverySourceLabel.
  ///
  /// In en, this message translates to:
  /// **'source'**
  String get discoverySourceLabel;

  /// No description provided for @discoveryRequiresConfigurationLabel.
  ///
  /// In en, this message translates to:
  /// **'requiresConfiguration'**
  String get discoveryRequiresConfigurationLabel;

  /// No description provided for @availabilityAvailable.
  ///
  /// In en, this message translates to:
  /// **'Available'**
  String get availabilityAvailable;

  /// No description provided for @availabilityUnavailable.
  ///
  /// In en, this message translates to:
  /// **'Unavailable'**
  String get availabilityUnavailable;

  /// No description provided for @availabilityUnconfigured.
  ///
  /// In en, this message translates to:
  /// **'Needs configuration'**
  String get availabilityUnconfigured;

  /// No description provided for @availabilityAuthenticationRequired.
  ///
  /// In en, this message translates to:
  /// **'Needs authentication'**
  String get availabilityAuthenticationRequired;

  /// No description provided for @availabilityPartial.
  ///
  /// In en, this message translates to:
  /// **'Partially available'**
  String get availabilityPartial;

  /// No description provided for @availabilityUnknown.
  ///
  /// In en, this message translates to:
  /// **'Unknown'**
  String get availabilityUnknown;

  /// No description provided for @localAgentScanDialogTitle.
  ///
  /// In en, this message translates to:
  /// **'Scan & import local agents'**
  String get localAgentScanDialogTitle;

  /// No description provided for @localAgentScanDialogDescription.
  ///
  /// In en, this message translates to:
  /// **'Passively scans local candidates, groups them by category, verifies with a bounded initialize-only handshake, and imports atomically.'**
  String get localAgentScanDialogDescription;

  /// No description provided for @localAgentRescan.
  ///
  /// In en, this message translates to:
  /// **'Rescan'**
  String get localAgentRescan;

  /// No description provided for @localAgentManualAdd.
  ///
  /// In en, this message translates to:
  /// **'Add manually'**
  String get localAgentManualAdd;

  /// No description provided for @localAgentSelectExecutable.
  ///
  /// In en, this message translates to:
  /// **'Select file to verify'**
  String get localAgentSelectExecutable;

  /// No description provided for @localAgentScanning.
  ///
  /// In en, this message translates to:
  /// **'Scanning…'**
  String get localAgentScanning;

  /// No description provided for @localAgentNoCandidates.
  ///
  /// In en, this message translates to:
  /// **'No local candidates were found.'**
  String get localAgentNoCandidates;

  /// No description provided for @localAgentCategoryAgent.
  ///
  /// In en, this message translates to:
  /// **'Agent'**
  String get localAgentCategoryAgent;

  /// No description provided for @localAgentCategoryModelRuntime.
  ///
  /// In en, this message translates to:
  /// **'Model Runtime'**
  String get localAgentCategoryModelRuntime;

  /// No description provided for @localAgentCategoryToolServer.
  ///
  /// In en, this message translates to:
  /// **'Tool Server'**
  String get localAgentCategoryToolServer;

  /// No description provided for @localAgentCategoryUnknown.
  ///
  /// In en, this message translates to:
  /// **'Unknown'**
  String get localAgentCategoryUnknown;

  /// No description provided for @localAgentGroupEmpty.
  ///
  /// In en, this message translates to:
  /// **'(no candidates)'**
  String get localAgentGroupEmpty;

  /// No description provided for @localAgentErrorShuttingDown.
  ///
  /// In en, this message translates to:
  /// **'The service is shutting down; please retry shortly.'**
  String get localAgentErrorShuttingDown;

  /// No description provided for @localAgentErrorIdentityChanged.
  ///
  /// In en, this message translates to:
  /// **'The candidate identity changed; rescan and try again.'**
  String get localAgentErrorIdentityChanged;

  /// No description provided for @localAgentErrorConflict.
  ///
  /// In en, this message translates to:
  /// **'The import conflicts with an existing record and cannot continue.'**
  String get localAgentErrorConflict;

  /// No description provided for @localAgentErrorPersistence.
  ///
  /// In en, this message translates to:
  /// **'The import could not be persisted.'**
  String get localAgentErrorPersistence;

  /// No description provided for @localAgentErrorCapacity.
  ///
  /// In en, this message translates to:
  /// **'Capacity is full right now; please retry later.'**
  String get localAgentErrorCapacity;

  /// No description provided for @localAgentErrorScanMissing.
  ///
  /// In en, this message translates to:
  /// **'The scan no longer exists or expired; rescan to continue.'**
  String get localAgentErrorScanMissing;

  /// No description provided for @localAgentErrorCandidateMissing.
  ///
  /// In en, this message translates to:
  /// **'The candidate no longer exists; rescan to continue.'**
  String get localAgentErrorCandidateMissing;

  /// No description provided for @localAgentErrorCandidateDismissed.
  ///
  /// In en, this message translates to:
  /// **'This candidate was hidden.'**
  String get localAgentErrorCandidateDismissed;

  /// No description provided for @localAgentErrorConsentRequired.
  ///
  /// In en, this message translates to:
  /// **'Verification consent is required first.'**
  String get localAgentErrorConsentRequired;

  /// No description provided for @localAgentErrorVerificationInProgress.
  ///
  /// In en, this message translates to:
  /// **'This candidate is being verified.'**
  String get localAgentErrorVerificationInProgress;

  /// No description provided for @localAgentErrorAdapterRequired.
  ///
  /// In en, this message translates to:
  /// **'This candidate needs an adapter.'**
  String get localAgentErrorAdapterRequired;

  /// No description provided for @localAgentErrorScanWorkerUnavailable.
  ///
  /// In en, this message translates to:
  /// **'The scan service is unavailable; please retry.'**
  String get localAgentErrorScanWorkerUnavailable;

  /// No description provided for @localAgentErrorPlanMismatch.
  ///
  /// In en, this message translates to:
  /// **'The import plan no longer matches the current selection; fetch it again.'**
  String get localAgentErrorPlanMismatch;

  /// No description provided for @localAgentErrorGeneric.
  ///
  /// In en, this message translates to:
  /// **'The operation failed; please retry.'**
  String get localAgentErrorGeneric;

  /// No description provided for @localAgentStatusDiscovery.
  ///
  /// In en, this message translates to:
  /// **'Discovery'**
  String get localAgentStatusDiscovery;

  /// No description provided for @localAgentStatusCompatibility.
  ///
  /// In en, this message translates to:
  /// **'Protocol'**
  String get localAgentStatusCompatibility;

  /// No description provided for @localAgentStatusAuth.
  ///
  /// In en, this message translates to:
  /// **'Auth'**
  String get localAgentStatusAuth;

  /// No description provided for @localAgentStatusHealth.
  ///
  /// In en, this message translates to:
  /// **'Health'**
  String get localAgentStatusHealth;

  /// No description provided for @localAgentDiscoveryObserved.
  ///
  /// In en, this message translates to:
  /// **'Observed'**
  String get localAgentDiscoveryObserved;

  /// No description provided for @localAgentDiscoveryIdentified.
  ///
  /// In en, this message translates to:
  /// **'Identified'**
  String get localAgentDiscoveryIdentified;

  /// No description provided for @localAgentDiscoveryDisappeared.
  ///
  /// In en, this message translates to:
  /// **'Disappeared'**
  String get localAgentDiscoveryDisappeared;

  /// No description provided for @localAgentCompatibilityCompatible.
  ///
  /// In en, this message translates to:
  /// **'Compatible'**
  String get localAgentCompatibilityCompatible;

  /// No description provided for @localAgentCompatibilityIncompatible.
  ///
  /// In en, this message translates to:
  /// **'Incompatible'**
  String get localAgentCompatibilityIncompatible;

  /// No description provided for @localAgentCompatibilityAdapterRequired.
  ///
  /// In en, this message translates to:
  /// **'Adapter required'**
  String get localAgentCompatibilityAdapterRequired;

  /// No description provided for @localAgentCompatibilityNotVerified.
  ///
  /// In en, this message translates to:
  /// **'Not verified'**
  String get localAgentCompatibilityNotVerified;

  /// No description provided for @localAgentAuthUnknown.
  ///
  /// In en, this message translates to:
  /// **'Unknown'**
  String get localAgentAuthUnknown;

  /// No description provided for @localAgentAuthNotRequired.
  ///
  /// In en, this message translates to:
  /// **'Not required'**
  String get localAgentAuthNotRequired;

  /// No description provided for @localAgentAuthRequired.
  ///
  /// In en, this message translates to:
  /// **'Sign-in required'**
  String get localAgentAuthRequired;

  /// No description provided for @localAgentAuthReady.
  ///
  /// In en, this message translates to:
  /// **'Ready'**
  String get localAgentAuthReady;

  /// No description provided for @localAgentHealthNotChecked.
  ///
  /// In en, this message translates to:
  /// **'Not checked'**
  String get localAgentHealthNotChecked;

  /// No description provided for @localAgentHealthReady.
  ///
  /// In en, this message translates to:
  /// **'Ready'**
  String get localAgentHealthReady;

  /// No description provided for @localAgentHealthUnavailable.
  ///
  /// In en, this message translates to:
  /// **'Unavailable'**
  String get localAgentHealthUnavailable;

  /// No description provided for @localAgentHealthIdentityMismatch.
  ///
  /// In en, this message translates to:
  /// **'Identity mismatch'**
  String get localAgentHealthIdentityMismatch;

  /// No description provided for @localAgentLifecycleObserved.
  ///
  /// In en, this message translates to:
  /// **'Observed'**
  String get localAgentLifecycleObserved;

  /// No description provided for @localAgentLifecycleIdentified.
  ///
  /// In en, this message translates to:
  /// **'Identified, awaiting verification'**
  String get localAgentLifecycleIdentified;

  /// No description provided for @localAgentLifecycleVerifying.
  ///
  /// In en, this message translates to:
  /// **'Verifying…'**
  String get localAgentLifecycleVerifying;

  /// No description provided for @localAgentLifecycleVerified.
  ///
  /// In en, this message translates to:
  /// **'Verified'**
  String get localAgentLifecycleVerified;

  /// No description provided for @localAgentLifecycleAuthRequired.
  ///
  /// In en, this message translates to:
  /// **'Auth required'**
  String get localAgentLifecycleAuthRequired;

  /// No description provided for @localAgentLifecycleIdentityChanged.
  ///
  /// In en, this message translates to:
  /// **'Identity changed; rescan to refresh'**
  String get localAgentLifecycleIdentityChanged;

  /// No description provided for @localAgentLifecycleNotVerified.
  ///
  /// In en, this message translates to:
  /// **'Not verified'**
  String get localAgentLifecycleNotVerified;

  /// No description provided for @localAgentVerifyConsentTitle.
  ///
  /// In en, this message translates to:
  /// **'Verify compatibility'**
  String get localAgentVerifyConsentTitle;

  /// No description provided for @localAgentVerifyConsentBody.
  ///
  /// In en, this message translates to:
  /// **'Verification runs one bounded protocol handshake (initialize only). No task, prompt, or tool call is sent; the verifier is isolated and time-boxed by Core.'**
  String get localAgentVerifyConsentBody;

  /// No description provided for @localAgentVerifyConsentAgree.
  ///
  /// In en, this message translates to:
  /// **'Agree & verify'**
  String get localAgentVerifyConsentAgree;

  /// No description provided for @localAgentVerify.
  ///
  /// In en, this message translates to:
  /// **'Verify'**
  String get localAgentVerify;

  /// No description provided for @localAgentImport.
  ///
  /// In en, this message translates to:
  /// **'Import'**
  String get localAgentImport;

  /// No description provided for @localAgentDismiss.
  ///
  /// In en, this message translates to:
  /// **'Hide'**
  String get localAgentDismiss;

  /// No description provided for @localAgentUnknownNeedsAdapter.
  ///
  /// In en, this message translates to:
  /// **'This candidate needs an adapter or manifest to be selected before it can be used.'**
  String get localAgentUnknownNeedsAdapter;

  /// No description provided for @localAgentModelRuntimeNote.
  ///
  /// In en, this message translates to:
  /// **'Model Runtime: this category needs a separate model-connector flow (not yet available).'**
  String get localAgentModelRuntimeNote;

  /// No description provided for @localAgentToolServerNote.
  ///
  /// In en, this message translates to:
  /// **'Tool Server: this category belongs in the tool center (not yet available).'**
  String get localAgentToolServerNote;

  /// No description provided for @localAgentImportReusedNotice.
  ///
  /// In en, this message translates to:
  /// **'This agent was already imported; the existing record was reused.'**
  String get localAgentImportReusedNotice;

  /// No description provided for @localAgentEventReplayGapNotice.
  ///
  /// In en, this message translates to:
  /// **'The event stream had a gap; the view fell back to snapshot refresh.'**
  String get localAgentEventReplayGapNotice;

  /// No description provided for @localAgentEventStreamNotice.
  ///
  /// In en, this message translates to:
  /// **'Event subscription is unavailable; the view is using snapshot refresh.'**
  String get localAgentEventStreamNotice;

  /// No description provided for @localAgentProjectRequired.
  ///
  /// In en, this message translates to:
  /// **'Select a project before importing an agent.'**
  String get localAgentProjectRequired;

  /// No description provided for @localAgentImportDialogTitle.
  ///
  /// In en, this message translates to:
  /// **'Import agent'**
  String get localAgentImportDialogTitle;

  /// No description provided for @localAgentImportTargetProject.
  ///
  /// In en, this message translates to:
  /// **'Target project: {projectId}'**
  String localAgentImportTargetProject(String projectId);

  /// No description provided for @localAgentModelSelectionTitle.
  ///
  /// In en, this message translates to:
  /// **'Model selection'**
  String get localAgentModelSelectionTitle;

  /// No description provided for @localAgentModelConnectorDefault.
  ///
  /// In en, this message translates to:
  /// **'Use the connector default model (no model ID)'**
  String get localAgentModelConnectorDefault;

  /// No description provided for @localAgentModelConnectorDefaultHint.
  ///
  /// In en, this message translates to:
  /// **'connector_default; omitting a model ID is a valid import option.'**
  String get localAgentModelConnectorDefaultHint;

  /// No description provided for @localAgentModelPinned.
  ///
  /// In en, this message translates to:
  /// **'Specify a model'**
  String get localAgentModelPinned;

  /// No description provided for @localAgentModelPinnedLabel.
  ///
  /// In en, this message translates to:
  /// **'Model'**
  String get localAgentModelPinnedLabel;

  /// No description provided for @localAgentModelPinnedUnavailable.
  ///
  /// In en, this message translates to:
  /// **'This candidate has no model list; use the connector default.'**
  String get localAgentModelPinnedUnavailable;

  /// No description provided for @localAgentImportPlanLoading.
  ///
  /// In en, this message translates to:
  /// **'Generating the read-only import plan…'**
  String get localAgentImportPlanLoading;

  /// No description provided for @localAgentImportPlanMissing.
  ///
  /// In en, this message translates to:
  /// **'The import plan is not available yet.'**
  String get localAgentImportPlanMissing;

  /// No description provided for @localAgentImportPlanSummary.
  ///
  /// In en, this message translates to:
  /// **'Import plan summary'**
  String get localAgentImportPlanSummary;

  /// No description provided for @localAgentImportPlanReadOnly.
  ///
  /// In en, this message translates to:
  /// **'Read-only'**
  String get localAgentImportPlanReadOnly;

  /// No description provided for @localAgentImportPlanConnector.
  ///
  /// In en, this message translates to:
  /// **'Connector'**
  String get localAgentImportPlanConnector;

  /// No description provided for @localAgentImportPlanAdapter.
  ///
  /// In en, this message translates to:
  /// **'Adapter'**
  String get localAgentImportPlanAdapter;

  /// No description provided for @localAgentImportPlanProtocol.
  ///
  /// In en, this message translates to:
  /// **'Protocol'**
  String get localAgentImportPlanProtocol;

  /// No description provided for @localAgentImportPlanAuth.
  ///
  /// In en, this message translates to:
  /// **'Auth'**
  String get localAgentImportPlanAuth;

  /// No description provided for @localAgentImportPlanAuthRequired.
  ///
  /// In en, this message translates to:
  /// **'Auth required'**
  String get localAgentImportPlanAuthRequired;

  /// No description provided for @localAgentImportPlanModel.
  ///
  /// In en, this message translates to:
  /// **'Model'**
  String get localAgentImportPlanModel;

  /// No description provided for @localAgentImportPlanActions.
  ///
  /// In en, this message translates to:
  /// **'Plan actions: '**
  String get localAgentImportPlanActions;

  /// No description provided for @localAgentImportConfirm.
  ///
  /// In en, this message translates to:
  /// **'Confirm import'**
  String get localAgentImportConfirm;

  /// No description provided for @localAgentImportDone.
  ///
  /// In en, this message translates to:
  /// **'Done'**
  String get localAgentImportDone;

  /// No description provided for @localAgentImportSuccess.
  ///
  /// In en, this message translates to:
  /// **'Import succeeded'**
  String get localAgentImportSuccess;

  /// No description provided for @localAgentImportSuccessReused.
  ///
  /// In en, this message translates to:
  /// **'Already imported (reused)'**
  String get localAgentImportSuccessReused;

  /// No description provided for @localAgentImportReceiptNote.
  ///
  /// In en, this message translates to:
  /// **'Created agent {agentId} on connector {connectorId}. A successful import does not mean a real agent turn has run.'**
  String localAgentImportReceiptNote(String agentId, String connectorId);

  /// No description provided for @localAgentEvidenceExecutableInventory.
  ///
  /// In en, this message translates to:
  /// **'Executable inventory'**
  String get localAgentEvidenceExecutableInventory;

  /// No description provided for @localAgentEvidenceWindowsPath.
  ///
  /// In en, this message translates to:
  /// **'On PATH'**
  String get localAgentEvidenceWindowsPath;

  /// No description provided for @localAgentEvidenceAppPaths.
  ///
  /// In en, this message translates to:
  /// **'Registered in App Paths'**
  String get localAgentEvidenceAppPaths;

  /// No description provided for @localAgentEvidencePackage.
  ///
  /// In en, this message translates to:
  /// **'Package inventory'**
  String get localAgentEvidencePackage;

  /// No description provided for @localAgentEvidenceLoopback.
  ///
  /// In en, this message translates to:
  /// **'Loopback service'**
  String get localAgentEvidenceLoopback;

  /// No description provided for @localAgentEvidenceUserSelected.
  ///
  /// In en, this message translates to:
  /// **'User selected'**
  String get localAgentEvidenceUserSelected;

  /// No description provided for @localAgentEvidenceRuntimeRecord.
  ///
  /// In en, this message translates to:
  /// **'Runtime record'**
  String get localAgentEvidenceRuntimeRecord;

  /// No description provided for @localAgentEvidenceVersionMatched.
  ///
  /// In en, this message translates to:
  /// **'Version matched'**
  String get localAgentEvidenceVersionMatched;

  /// No description provided for @localAgentEvidenceBuildMatched.
  ///
  /// In en, this message translates to:
  /// **'Build matched'**
  String get localAgentEvidenceBuildMatched;

  /// No description provided for @localAgentEvidenceInstallKnown.
  ///
  /// In en, this message translates to:
  /// **'Known install'**
  String get localAgentEvidenceInstallKnown;

  /// No description provided for @localAgentEvidenceAvailable.
  ///
  /// In en, this message translates to:
  /// **'Available'**
  String get localAgentEvidenceAvailable;

  /// No description provided for @localAgentEvidenceAuthRequired.
  ///
  /// In en, this message translates to:
  /// **'Auth required'**
  String get localAgentEvidenceAuthRequired;

  /// No description provided for @localAgentEvidenceUnconfigured.
  ///
  /// In en, this message translates to:
  /// **'Needs configuration'**
  String get localAgentEvidenceUnconfigured;

  /// No description provided for @localAgentEvidenceIdentityMismatch.
  ///
  /// In en, this message translates to:
  /// **'Identity mismatch'**
  String get localAgentEvidenceIdentityMismatch;

  /// No description provided for @localAgentEvidenceCatalogUnavailable.
  ///
  /// In en, this message translates to:
  /// **'Catalog unavailable'**
  String get localAgentEvidenceCatalogUnavailable;

  /// No description provided for @connectorDiscoverEmptyTitle.
  ///
  /// In en, this message translates to:
  /// **'No connectors discovered yet'**
  String get connectorDiscoverEmptyTitle;

  /// No description provided for @connectorDiscoverEmptySubtitle.
  ///
  /// In en, this message translates to:
  /// **'Refresh to run connector.discover and inspect local candidates.'**
  String get connectorDiscoverEmptySubtitle;

  /// No description provided for @connectorDiscoverScannning.
  ///
  /// In en, this message translates to:
  /// **'Discovering connectors…'**
  String get connectorDiscoverScannning;

  /// No description provided for @connectorDiscoverFailed.
  ///
  /// In en, this message translates to:
  /// **'Connector discovery failed: '**
  String get connectorDiscoverFailed;

  /// No description provided for @connectorDiscoverRetry.
  ///
  /// In en, this message translates to:
  /// **'Retry'**
  String get connectorDiscoverRetry;

  /// No description provided for @connectorDiscoverRescan.
  ///
  /// In en, this message translates to:
  /// **'Refresh'**
  String get connectorDiscoverRescan;

  /// No description provided for @connectorDiscoverNotFound.
  ///
  /// In en, this message translates to:
  /// **'No local connectors were discovered.'**
  String get connectorDiscoverNotFound;

  /// No description provided for @connectorDiscoverSupported.
  ///
  /// In en, this message translates to:
  /// **'Discovered local connector candidates'**
  String get connectorDiscoverSupported;

  /// No description provided for @connectorDiscoverManualFallback.
  ///
  /// In en, this message translates to:
  /// **'Manage profiles'**
  String get connectorDiscoverManualFallback;

  /// No description provided for @advancedDiagnosticsTitle.
  ///
  /// In en, this message translates to:
  /// **'Advanced diagnostics'**
  String get advancedDiagnosticsTitle;

  /// No description provided for @advancedDiagnosticsSubtitle.
  ///
  /// In en, this message translates to:
  /// **'Runtime status and projection metadata'**
  String get advancedDiagnosticsSubtitle;

  /// No description provided for @technicalDiagnosticsDetails.
  ///
  /// In en, this message translates to:
  /// **'Technical diagnostics details'**
  String get technicalDiagnosticsDetails;

  /// No description provided for @retryStartup.
  ///
  /// In en, this message translates to:
  /// **'Retry startup'**
  String get retryStartup;

  /// No description provided for @coreHealth.
  ///
  /// In en, this message translates to:
  /// **'Core health'**
  String get coreHealth;

  /// No description provided for @coreProjectionReady.
  ///
  /// In en, this message translates to:
  /// **'Core projection ready'**
  String get coreProjectionReady;

  /// No description provided for @coreProjectionUnavailable.
  ///
  /// In en, this message translates to:
  /// **'Core projection unavailable'**
  String get coreProjectionUnavailable;

  /// No description provided for @coreEventStreamError.
  ///
  /// In en, this message translates to:
  /// **'Core event stream error: '**
  String get coreEventStreamError;

  /// No description provided for @coreProjectionReconnected.
  ///
  /// In en, this message translates to:
  /// **'Core projection reconnected'**
  String get coreProjectionReconnected;

  /// No description provided for @coreEventStreamStopped.
  ///
  /// In en, this message translates to:
  /// **'Event subscription failed; the app has stopped applying events.'**
  String get coreEventStreamStopped;

  /// No description provided for @coreEventRecoveryFailed.
  ///
  /// In en, this message translates to:
  /// **'Event recovery failed; fail-closed remains in effect.'**
  String get coreEventRecoveryFailed;

  /// No description provided for @projectHasNoAgents.
  ///
  /// In en, this message translates to:
  /// **'This project has no agents yet.'**
  String get projectHasNoAgents;

  /// No description provided for @projectAgentEmptyHint.
  ///
  /// In en, this message translates to:
  /// **'Add or scan an agent to make it appear here.'**
  String get projectAgentEmptyHint;

  /// No description provided for @scanLocalAgentsTitle.
  ///
  /// In en, this message translates to:
  /// **'Scan local agents'**
  String get scanLocalAgentsTitle;

  /// No description provided for @scanLocalAgentsDescription.
  ///
  /// In en, this message translates to:
  /// **'This call runs agent.scan_local and never auto-creates an identity.'**
  String get scanLocalAgentsDescription;

  /// No description provided for @searchMessagesHint.
  ///
  /// In en, this message translates to:
  /// **'Search the current conversation history'**
  String get searchMessagesHint;

  /// No description provided for @searchMessagesEmpty.
  ///
  /// In en, this message translates to:
  /// **'Enter a keyword to search messages'**
  String get searchMessagesEmpty;

  /// No description provided for @searchMessagesFailed.
  ///
  /// In en, this message translates to:
  /// **'Search failed: '**
  String get searchMessagesFailed;

  /// No description provided for @composerTools.
  ///
  /// In en, this message translates to:
  /// **'Composer tools'**
  String get composerTools;

  /// No description provided for @send.
  ///
  /// In en, this message translates to:
  /// **'Send'**
  String get send;

  /// No description provided for @stopActiveRun.
  ///
  /// In en, this message translates to:
  /// **'Stop active run'**
  String get stopActiveRun;

  /// No description provided for @attachment.
  ///
  /// In en, this message translates to:
  /// **'Attachment'**
  String get attachment;

  /// No description provided for @memory.
  ///
  /// In en, this message translates to:
  /// **'Memory'**
  String get memory;

  /// No description provided for @saveMemorySource.
  ///
  /// In en, this message translates to:
  /// **'Save memory'**
  String get saveMemorySource;

  /// No description provided for @retrieval.
  ///
  /// In en, this message translates to:
  /// **'Retrieval'**
  String get retrieval;

  /// No description provided for @saveRetrievalSource.
  ///
  /// In en, this message translates to:
  /// **'Save retrieval source'**
  String get saveRetrievalSource;

  /// No description provided for @agentPicker.
  ///
  /// In en, this message translates to:
  /// **'Select agent'**
  String get agentPicker;

  /// No description provided for @agentPanel.
  ///
  /// In en, this message translates to:
  /// **'Agent panel'**
  String get agentPanel;

  /// No description provided for @workflowPanel.
  ///
  /// In en, this message translates to:
  /// **'Workflow panel'**
  String get workflowPanel;

  /// No description provided for @toggleTheme.
  ///
  /// In en, this message translates to:
  /// **'Toggle theme'**
  String get toggleTheme;

  /// No description provided for @project.
  ///
  /// In en, this message translates to:
  /// **'Project'**
  String get project;

  /// No description provided for @conversation.
  ///
  /// In en, this message translates to:
  /// **'Conversation'**
  String get conversation;
}

class _AppLocalizationsDelegate
    extends LocalizationsDelegate<AppLocalizations> {
  const _AppLocalizationsDelegate();

  @override
  Future<AppLocalizations> load(Locale locale) {
    return SynchronousFuture<AppLocalizations>(lookupAppLocalizations(locale));
  }

  @override
  bool isSupported(Locale locale) =>
      <String>['en', 'zh'].contains(locale.languageCode);

  @override
  bool shouldReload(_AppLocalizationsDelegate old) => false;
}

AppLocalizations lookupAppLocalizations(Locale locale) {
  // Lookup logic when only language code is specified.
  switch (locale.languageCode) {
    case 'en':
      return AppLocalizationsEn();
    case 'zh':
      return AppLocalizationsZh();
  }

  throw FlutterError(
    'AppLocalizations.delegate failed to load unsupported locale "$locale". This is likely '
    'an issue with the localizations generation tool. Please file an issue '
    'on GitHub with a reproducible sample app and the gen-l10n configuration '
    'that was used.',
  );
}
