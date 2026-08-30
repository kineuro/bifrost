# Installs the bifrost command line tool on Windows (PowerShell 5 or later).
#   irm https://bifrost.kineuro.se/get.ps1 | iex
$ErrorActionPreference = 'Stop'
$base = if ($env:BIFROST_URL) { $env:BIFROST_URL } else { 'https://bifrost.kineuro.se' }
$arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq 'Arm64') { 'arm64' } else { 'amd64' }
$file = "bifrost-windows-$arch.exe"
$dir = Join-Path $env:LOCALAPPDATA 'Programs\bifrost'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$dest = Join-Path $dir 'bifrost.exe'
Write-Host "downloading $base/dl/$file"
Invoke-WebRequest -Uri "$base/dl/$file" -OutFile "$dest.tmp" -UseBasicParsing
$sums = (Invoke-WebRequest -Uri "$base/dl/SHA256SUMS" -UseBasicParsing).Content
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
