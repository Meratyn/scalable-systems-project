# Keeps Windows from going to sleep while a WSL benchmark runs (no power settings are changed).
# Windows drops the request as soon as this process exits. A closed laptop lid can still sleep.
# Usage (from WSL): powershell.exe -NoProfile -ExecutionPolicy Bypass -File <windows-path>\keep_awake.ps1 -Minutes 90
param([int]$Minutes = 90)
$sig = '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint f);'
$k = Add-Type -MemberDefinition $sig -Name Power -Namespace Win -PassThru
# ES_CONTINUOUS | ES_SYSTEM_REQUIRED
$null = $k::SetThreadExecutionState([uint32]"0x80000001")
Write-Output "keep-awake on for at most $Minutes min"
Start-Sleep -Seconds ($Minutes * 60)
