# Installs the bifrost command line tool on Windows (PowerShell 5 or later).
#   irm https://bifrost.kineuro.se/get.ps1 | iex
$ErrorActionPreference = 'Stop'
$base = if ($env:BIFROST_URL) { $env:BIFROST_URL } else { 'https://bifrost.kineuro.se' }
$arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq 'Arm64') { 'arm64' } else { 'amd64' }
$file = "bifrost-windows-$arch.exe"
$rel = 'https://github.com/kineuro/bifrost/releases/latest/download'
$dir = Join-Path $env:LOCALAPPDATA 'Programs\bifrost'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$dest = Join-Path $dir 'bifrost.exe'
try { Invoke-WebRequest -Uri "$rel/$file" -OutFile "$dest.tmp" -UseBasicParsing -TimeoutSec 300; $src = $rel; Write-Host "downloaded $file from the latest GitHub release" }
catch { $src = "$base/dl"; Write-Host "downloading $src/$file"; Invoke-WebRequest -Uri "$src/$file" -OutFile "$dest.tmp" -UseBasicParsing }
$sums = (Invoke-WebRequest -Uri "$src/SHA256SUMS" -UseBasicParsing).Content
$line = ($sums -split "`n") | Where-Object { $_ -match " $file$" }
if ($line) {
  $want = ($line -split '\s+')[0]
  $have = (Get-FileHash "$dest.tmp" -Algorithm SHA256).Hash.ToLower()
  if ($have -ne $want) { Remove-Item "$dest.tmp"; throw 'checksum mismatch, aborting' }
}
Move-Item -Force "$dest.tmp" $dest
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ($userPath -notlike "*$dir*") { [Environment]::SetEnvironmentVariable('Path', "$userPath;$dir", 'User'); $env:Path += ";$dir"; Write-Host "added $dir to your PATH (open a new terminal to use it)" }
Write-Host "installed $dest"
Write-Host 'next: bifrost login <token>   then   bifrost push <folder>   or   bifrost pull <folder>'
