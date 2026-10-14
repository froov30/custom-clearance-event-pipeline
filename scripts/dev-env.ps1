# Dot-source in every dev shell:  . .\scripts\dev-env.ps1
# Redirects every cache, temp and config location this project touches to D: (DECISIONS.md D-040).
# Session-only: no user or system environment variables are changed, and nothing is written to C:.
# No global $ErrorActionPreference: this file is dot-sourced, and 'Stop' would leak into the shell and make
# PowerShell 5.1 treat native tools' stderr progress (uv, docker) as fatal errors.
$Data = 'D:\ccp-data'
$Repo = Split-Path -Parent $PSScriptRoot

$dirs = @{
    UV_CACHE_DIR          = "$Data\uv-cache"
    UV_PYTHON_INSTALL_DIR = "$Data\uv-python"
    PIP_CACHE_DIR         = "$Data\pip-cache"
    TEMP                  = "$Data\tmp"
    TMP                   = "$Data\tmp"
    DOCKER_CONFIG         = "$Data\docker-config"
    KUBECACHEDIR          = "$Data\kube\cache"
}
foreach ($k in $dirs.Keys) {
    New-Item -ItemType Directory -Force -Path $dirs[$k] -ErrorAction Stop | Out-Null
    Set-Item -Path "Env:$k" -Value $dirs[$k]
}
New-Item -ItemType Directory -Force -Path "$Data\bin" | Out-Null
$env:KUBECONFIG = "$Data\kube\config"

# Docker Desktop keeps its engine contexts in %USERPROFILE%\.docker. Copy them once (read-only from C:)
# so the redirected DOCKER_CONFIG still finds the desktop-linux engine.
$src = Join-Path $env:USERPROFILE '.docker'
if ((Test-Path $src) -and -not (Test-Path "$($env:DOCKER_CONFIG)\config.json")) {
    Copy-Item "$src\config.json" $env:DOCKER_CONFIG -ErrorAction SilentlyContinue
    if (Test-Path "$src\contexts") { Copy-Item "$src\contexts" $env:DOCKER_CONFIG -Recurse }
}
# The CLI looks for plugins (compose, buildx) under DOCKER_CONFIG, so point it back at Docker Desktop's, read-only.
$cfgPath = "$($env:DOCKER_CONFIG)\config.json"
if ((Test-Path $cfgPath) -and -not (Select-String -Quiet -Pattern 'cliPluginsExtraDirs' -Path $cfgPath)) {
    $j = Get-Content $cfgPath -Raw | ConvertFrom-Json
    $j | Add-Member -NotePropertyName cliPluginsExtraDirs -NotePropertyValue @("$src\cli-plugins")
    # ASCII, not PowerShell 5.1's UTF-8 with BOM, which Docker's JSON parser rejects.
    $j | ConvertTo-Json -Depth 10 | Set-Content $cfgPath -Encoding ascii
}

# Load .env (git-ignored) so host services and tests see the same values Compose uses (D-044).
if (Test-Path "$Repo\.env") {
    Get-Content "$Repo\.env" | ForEach-Object {
        if ($_ -match '^\s*([A-Z_][A-Z0-9_]*)=(.*)$') { Set-Item -Path "Env:$($Matches[1])" -Value $Matches[2] }
    }
}

$env:PATH = "$Data\bin;$env:PATH"
if (Test-Path "$Repo\.venv\Scripts\Activate.ps1") { . "$Repo\.venv\Scripts\Activate.ps1" }
Write-Host "ccp dev env: data=$Data venv=$(Test-Path "$Repo\.venv")"
