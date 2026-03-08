# Keiba Ledger — Windowsタスクスケジューラ設定
# 管理者権限で実行: Start-Process powershell -Verb RunAs
# 設計書 2-6

$ProjectRoot = "C:\Users\Dice\keiba-ledger"  # プロジェクトルートに変更すること
$PythonExe = "py"  # または "python" / "py -3.11-32"
$ScriptPath = Join-Path $ProjectRoot "scripts\fetch\incremental_fetch.py"

$action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "-3.11-32 `"$ScriptPath`"" `
    -WorkingDirectory $ProjectRoot

$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday `
    -At "06:00AM"

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName "KeibaDataFetch" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest

Write-Host "タスク 'KeibaDataFetch' を登録しました（毎週月曜6:00）"
Write-Host "ProjectRoot: $ProjectRoot"
Write-Host "手動実行: schtasks /run /tn KeibaDataFetch"
