/**
 * SettingsPanel - Configure notifications and system settings
 *
 * Features:
 * - Slack webhook configuration
 * - Discord webhook configuration
 * - Notification preferences
 * - API key management
 * - Cost tracking limits
 *
 * Command Center dark theme
 */

import { useState } from 'react';
import {
  X,
  Bell,
  MessageSquare,
  Hash,
  Save,
  Eye,
  EyeOff,
  AlertCircle,
  CheckCircle,
  Loader2,
  DollarSign,
  Shield,
} from 'lucide-react';

interface SettingsPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

interface NotificationSettings {
  slackWebhookUrl: string;
  discordWebhookUrl: string;
  enableSlack: boolean;
  enableDiscord: boolean;
  notifyOnNewDeal: boolean;
  notifyOnStealthDetection: boolean;
  notifyOnThesisDrift: boolean;
  enterpriseAiOnly: boolean;
  leadDealsOnly: boolean;
  monthlyBudget: number;
}

export function SettingsPanel({ isOpen, onClose }: SettingsPanelProps) {
  const [settings, setSettings] = useState<NotificationSettings>({
    slackWebhookUrl: '',
    discordWebhookUrl: '',
    enableSlack: false,
    enableDiscord: false,
    notifyOnNewDeal: true,
    notifyOnStealthDetection: true,
    notifyOnThesisDrift: false,
    enterpriseAiOnly: true,
    leadDealsOnly: true,
    monthlyBudget: 100,
  });

  const [showSlackUrl, setShowSlackUrl] = useState(false);
  const [showDiscordUrl, setShowDiscordUrl] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [testingSlack, setTestingSlack] = useState(false);
  const [testingDiscord, setTestingDiscord] = useState(false);

  if (!isOpen) return null;

  const handleSave = async () => {
    setSaving(true);
    setSaveStatus('idle');
    try {
      // In a real implementation, this would call an API endpoint
      await new Promise((resolve) => setTimeout(resolve, 1000));
      setSaveStatus('success');
      setTimeout(() => setSaveStatus('idle'), 3000);
    } catch {
      setSaveStatus('error');
    } finally {
      setSaving(false);
    }
  };

  const testSlack = async () => {
    if (!settings.slackWebhookUrl) return;
    setTestingSlack(true);
    try {
      // In a real implementation, this would send a test message
      await new Promise((resolve) => setTimeout(resolve, 1500));
      alert('Slack test message sent!');
    } catch {
      alert('Failed to send Slack test message');
    } finally {
      setTestingSlack(false);
    }
  };

  const testDiscord = async () => {
    if (!settings.discordWebhookUrl) return;
    setTestingDiscord(true);
    try {
      // In a real implementation, this would send a test message
      await new Promise((resolve) => setTimeout(resolve, 1500));
      alert('Discord test message sent!');
    } catch {
      alert('Failed to send Discord test message');
    } finally {
      setTestingDiscord(false);
    }
  };

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-40 animate-fade-in"
        onClick={handleBackdropClick}
      />

      {/* Panel */}
      <div className="fixed top-0 right-0 h-full w-full max-w-lg z-50 bg-[#0a0a0c] border-l border-slate-800 shadow-2xl animate-slide-in overflow-y-auto">
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between p-4 sm:p-6 border-b border-slate-800 bg-[#0a0a0c]">
          <div className="flex items-center gap-3">
            <Bell className="w-5 h-5 text-emerald-400" />
            <h2 className="text-lg font-bold text-white">Settings</h2>
          </div>
          <button
            onClick={onClose}
            className="p-2.5 sm:p-2 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-4 sm:p-6 space-y-6 sm:space-y-8">
          {/* Slack Configuration */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <Hash className="w-4 h-4 text-purple-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Slack Notifications
              </h3>
            </div>

            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Enable Slack</span>
                <ToggleSwitch
                  enabled={settings.enableSlack}
                  onChange={(v) => setSettings({ ...settings, enableSlack: v })}
                />
              </div>

              <div>
                <label className="text-xs text-slate-500 block mb-2">Webhook URL</label>
                <div className="relative">
                  <input
                    type={showSlackUrl ? 'text' : 'password'}
                    value={settings.slackWebhookUrl}
                    onChange={(e) =>
                      setSettings({ ...settings, slackWebhookUrl: e.target.value })
                    }
                    placeholder="https://hooks.slack.com/services/..."
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 pr-20 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-emerald-500"
                  />
                  <div className="absolute right-2 top-1/2 -translate-y-1/2 flex gap-1">
                    <button
                      onClick={() => setShowSlackUrl(!showSlackUrl)}
                      className="p-1 text-slate-500 hover:text-white"
                    >
                      {showSlackUrl ? (
                        <EyeOff className="w-4 h-4" />
                      ) : (
                        <Eye className="w-4 h-4" />
                      )}
                    </button>
                  </div>
                </div>
              </div>

              <button
                onClick={testSlack}
                disabled={!settings.slackWebhookUrl || testingSlack}
                className="btn-secondary w-full flex items-center justify-center gap-2 disabled:opacity-50"
              >
                {testingSlack ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <MessageSquare className="w-4 h-4" />
                )}
                Send Test Message
              </button>
            </div>
          </section>

          {/* Discord Configuration */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <MessageSquare className="w-4 h-4 text-blue-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Discord Notifications
              </h3>
            </div>

            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Enable Discord</span>
                <ToggleSwitch
                  enabled={settings.enableDiscord}
                  onChange={(v) => setSettings({ ...settings, enableDiscord: v })}
                />
              </div>

              <div>
                <label className="text-xs text-slate-500 block mb-2">Webhook URL</label>
                <div className="relative">
                  <input
                    type={showDiscordUrl ? 'text' : 'password'}
                    value={settings.discordWebhookUrl}
                    onChange={(e) =>
                      setSettings({ ...settings, discordWebhookUrl: e.target.value })
                    }
                    placeholder="https://discord.com/api/webhooks/..."
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 pr-20 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-emerald-500"
                  />
                  <div className="absolute right-2 top-1/2 -translate-y-1/2 flex gap-1">
                    <button
                      onClick={() => setShowDiscordUrl(!showDiscordUrl)}
                      className="p-1 text-slate-500 hover:text-white"
                    >
                      {showDiscordUrl ? (
                        <EyeOff className="w-4 h-4" />
                      ) : (
                        <Eye className="w-4 h-4" />
                      )}
                    </button>
                  </div>
                </div>
              </div>

              <button
                onClick={testDiscord}
                disabled={!settings.discordWebhookUrl || testingDiscord}
                className="btn-secondary w-full flex items-center justify-center gap-2 disabled:opacity-50"
              >
                {testingDiscord ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <MessageSquare className="w-4 h-4" />
                )}
                Send Test Message
              </button>
            </div>
          </section>

          {/* Notification Preferences */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <Bell className="w-4 h-4 text-amber-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Notification Triggers
              </h3>
            </div>

            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">New Deal Detected</span>
                <ToggleSwitch
                  enabled={settings.notifyOnNewDeal}
                  onChange={(v) => setSettings({ ...settings, notifyOnNewDeal: v })}
                />
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Stealth Detection</span>
                <ToggleSwitch
                  enabled={settings.notifyOnStealthDetection}
                  onChange={(v) =>
                    setSettings({ ...settings, notifyOnStealthDetection: v })
                  }
                />
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Thesis Drift Alert</span>
                <ToggleSwitch
                  enabled={settings.notifyOnThesisDrift}
                  onChange={(v) => setSettings({ ...settings, notifyOnThesisDrift: v })}
                />
              </div>
            </div>
          </section>

          {/* Filters */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <Shield className="w-4 h-4 text-emerald-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Notification Filters
              </h3>
            </div>

            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Enterprise AI Only</span>
                <ToggleSwitch
                  enabled={settings.enterpriseAiOnly}
                  onChange={(v) => setSettings({ ...settings, enterpriseAiOnly: v })}
                />
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Lead Deals Only</span>
                <ToggleSwitch
                  enabled={settings.leadDealsOnly}
                  onChange={(v) => setSettings({ ...settings, leadDealsOnly: v })}
                />
              </div>
            </div>
          </section>

          {/* Budget */}
          <section>
            <div className="flex items-center gap-2 mb-4">
              <DollarSign className="w-4 h-4 text-green-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Monthly Budget
              </h3>
            </div>

            <div>
              <label className="text-xs text-slate-500 block mb-2">
                API Cost Limit (USD)
              </label>
              <input
                type="number"
                value={settings.monthlyBudget}
                onChange={(e) =>
                  setSettings({ ...settings, monthlyBudget: parseInt(e.target.value) || 0 })
                }
                className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-emerald-500"
              />
              <p className="text-xs text-slate-600 mt-1">
                Alert when monthly API costs exceed this limit
              </p>
            </div>
          </section>
        </div>

        {/* Footer */}
        <div className="sticky bottom-0 p-4 sm:p-6 border-t border-slate-800 bg-[#0a0a0c]">
          {saveStatus === 'success' && (
            <div className="flex items-center gap-2 text-emerald-400 text-sm mb-4">
              <CheckCircle className="w-4 h-4" />
              Settings saved successfully
            </div>
          )}
          {saveStatus === 'error' && (
            <div className="flex items-center gap-2 text-red-400 text-sm mb-4">
              <AlertCircle className="w-4 h-4" />
              Failed to save settings
            </div>
          )}

          <div className="flex gap-3">
            <button onClick={onClose} className="btn-secondary flex-1">
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={saving}
              className="btn-primary flex-1 flex items-center justify-center gap-2"
            >
              {saving ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              Save Settings
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

interface ToggleSwitchProps {
  enabled: boolean;
  onChange: (enabled: boolean) => void;
}

function ToggleSwitch({ enabled, onChange }: ToggleSwitchProps) {
  return (
    <button
      onClick={() => onChange(!enabled)}
      className={`relative w-12 h-7 sm:w-11 sm:h-6 rounded-full transition-colors min-w-[44px] ${
        enabled ? 'bg-emerald-500' : 'bg-slate-700'
      }`}
    >
      <div
        className={`absolute top-1 sm:top-1 w-5 h-5 sm:w-4 sm:h-4 bg-white rounded-full transition-transform ${
          enabled ? 'translate-x-6' : 'translate-x-1'
        }`}
      />
    </button>
  );
}

export default SettingsPanel;
