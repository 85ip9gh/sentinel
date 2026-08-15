# Register the collector as a Windows scheduled task that starts at logon.
#
#   powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1
#
# Registered for the current user, so no elevation is needed. The broker runs
# on this machine under Docker Desktop, which also starts at logon, and the
# collector spools to disk while it waits for the broker to come up.

param(
    [string]$TaskName = "Sentinel Collector",
    [string]$HostName = $env:COMPUTERNAME.ToLower(),
    [string]$Role = "workstation"
)

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$wrapper = Join-Path $PSScriptRoot "run-collector.cmd"

if (-not (Test-Path (Join-Path $repo ".venv\Scripts\python.exe"))) {
    throw "No virtualenv at $repo\.venv. Create it and install requirements.txt first."
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" `
    -Argument "/c `"$wrapper`"" -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Description "Publishes host telemetry to the Sentinel Kafka broker." `
    -Force | Out-Null

# The wrapper reads these from the task's own environment at run time.
[Environment]::SetEnvironmentVariable("SENTINEL_HOST", $HostName, "User")
[Environment]::SetEnvironmentVariable("SENTINEL_ROLE", $Role, "User")

Start-ScheduledTask -TaskName $TaskName
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
